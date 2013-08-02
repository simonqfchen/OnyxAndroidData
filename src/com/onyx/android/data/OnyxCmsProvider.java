/**
 * 
 */
package com.onyx.android.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.ParcelFileDescriptor;
import android.text.TextUtils;
import android.util.Log;

import com.onyx.android.sdk.data.cms.OnyxAnnotation;
import com.onyx.android.sdk.data.cms.OnyxBookmark;
import com.onyx.android.sdk.data.cms.OnyxCmsCenter;
import com.onyx.android.sdk.data.cms.OnyxLibraryItem;
import com.onyx.android.sdk.data.cms.OnyxMetadata;
import com.onyx.android.sdk.data.cms.OnyxHistoryEntry;
import com.onyx.android.sdk.data.cms.OnyxThumbnail;
import com.onyx.android.sdk.device.EnvironmentUtil;

/**
 * @author joy
 *
 */
public class OnyxCmsProvider extends ContentProvider
{
    private static final String TAG = "OnyxCmsProvider";
    
    private static final String sDBName = "onyx.cms.db";
    private static final int sDBVersion = 3;
    
    private static HashMap<String, String> sItemProjectionMap;
    private static HashMap<String, String> sMetadataProjectionMap;
    private static HashMap<String, String> sBookmarkProjectionMap;
    private static HashMap<String, String> sAnnotationProjectionMap;
    private static HashMap<String, String> sThumbnailProjectionMap;
    private static HashMap<String, String> sHistoryProjectionMap;
    
    private static final UriMatcher sUriMatcher;
    private static class MatcherResult {
        public static final int ITEMS = 1;
        public static final int ITEM_ID = 2;
        public static final int METADATAS = 3;
        public static final int METADATA_ID = 4;
        public static final int BOOKMARKS = 5;
        public static final int BOOKMARK_ID = 6;
        public static final int ANNOTATIONS = 7;
        public static final int ANNOTATION_ID = 8;
        public static final int THUMBNAILS = 9;
        public static final int THUMBNAIL_ID = 10;
        public static final int HISTORIES = 11;
        public static final int HISTORY_ID = 12;
    }
    
    static {
        sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxLibraryItem.DB_TABLE_NAME, MatcherResult.ITEMS);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxLibraryItem.DB_TABLE_NAME + "/#", MatcherResult.ITEM_ID);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxMetadata.DB_TABLE_NAME, MatcherResult.METADATAS);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxMetadata.DB_TABLE_NAME + "/#", MatcherResult.METADATA_ID);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxBookmark.DB_TABLE_NAME, MatcherResult.BOOKMARKS);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxBookmark.DB_TABLE_NAME + "/#", MatcherResult.BOOKMARK_ID);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxAnnotation.DB_TABLE_NAME, MatcherResult.ANNOTATIONS);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxAnnotation.DB_TABLE_NAME + "/#", MatcherResult.ANNOTATION_ID);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxThumbnail.DB_TABLE_NAME, MatcherResult.THUMBNAILS);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxThumbnail.DB_TABLE_NAME + "/#", MatcherResult.THUMBNAIL_ID);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxHistoryEntry.DB_TABLE_NAME, MatcherResult.HISTORIES);
        sUriMatcher.addURI(OnyxCmsCenter.PROVIDER_AUTHORITY, OnyxHistoryEntry.DB_TABLE_NAME + "/#", MatcherResult.HISTORY_ID);
        
        sItemProjectionMap = new HashMap<String, String>();
        sItemProjectionMap.put(OnyxLibraryItem.Columns._ID, OnyxLibraryItem.Columns._ID);
        sItemProjectionMap.put(OnyxLibraryItem.Columns.PATH, OnyxLibraryItem.Columns.PATH);
        sItemProjectionMap.put(OnyxLibraryItem.Columns.NAME, OnyxLibraryItem.Columns.NAME);
        sItemProjectionMap.put(OnyxLibraryItem.Columns.SIZE, OnyxLibraryItem.Columns.SIZE);
        sItemProjectionMap.put(OnyxLibraryItem.Columns.TYPE, OnyxLibraryItem.Columns.TYPE);
        sItemProjectionMap.put(OnyxLibraryItem.Columns.LAST_CHANGE, OnyxLibraryItem.Columns.LAST_CHANGE);
        
        sMetadataProjectionMap = new HashMap<String, String>();
        sMetadataProjectionMap.put(OnyxMetadata.Columns._ID, OnyxMetadata.Columns._ID);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.MD5, OnyxMetadata.Columns.MD5);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.NAME, OnyxMetadata.Columns.NAME);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.TITLE, OnyxMetadata.Columns.TITLE);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.AUTHORS, OnyxMetadata.Columns.AUTHORS);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.PUBLISHER, OnyxMetadata.Columns.PUBLISHER);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.LANGUAGE, OnyxMetadata.Columns.LANGUAGE);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.DESCRIPTION, OnyxMetadata.Columns.DESCRIPTION);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.LOCATION, OnyxMetadata.Columns.LOCATION);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.NATIVE_ABSOLUTE_PATH, OnyxMetadata.Columns.NATIVE_ABSOLUTE_PATH);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.SIZE, OnyxMetadata.Columns.SIZE);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.ENCODING, OnyxMetadata.Columns.ENCODING);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.LAST_ACCESS, OnyxMetadata.Columns.LAST_ACCESS);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.LAST_MODIFIED, OnyxMetadata.Columns.LAST_MODIFIED);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.PROGRESS, OnyxMetadata.Columns.PROGRESS);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.FAVORITE, OnyxMetadata.Columns.FAVORITE);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.RATING, OnyxMetadata.Columns.RATING);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.TAGS, OnyxMetadata.Columns.TAGS);
        sMetadataProjectionMap.put(OnyxMetadata.Columns.EXTRA_ATTRIBUTES, OnyxMetadata.Columns.EXTRA_ATTRIBUTES);
        
        sBookmarkProjectionMap = new HashMap<String, String>();
        sBookmarkProjectionMap.put(OnyxBookmark.Columns._ID, OnyxBookmark.Columns._ID);
        sBookmarkProjectionMap.put(OnyxBookmark.Columns.MD5, OnyxBookmark.Columns.MD5);
        sBookmarkProjectionMap.put(OnyxBookmark.Columns.QUOTE, OnyxBookmark.Columns.QUOTE);
        sBookmarkProjectionMap.put(OnyxBookmark.Columns.LOCATION, OnyxBookmark.Columns.LOCATION);
        sBookmarkProjectionMap.put(OnyxBookmark.Columns.UPDATE_TIME, OnyxBookmark.Columns.UPDATE_TIME);
        
        sAnnotationProjectionMap = new HashMap<String, String>();
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns._ID, OnyxAnnotation.Columns._ID);
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns.MD5, OnyxAnnotation.Columns.MD5);
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns.QUOTE, OnyxAnnotation.Columns.QUOTE);
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns.LOCATION_BEGIN, OnyxAnnotation.Columns.LOCATION_BEGIN);
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns.LOCATION_END, OnyxAnnotation.Columns.LOCATION_END);
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns.NOTE, OnyxAnnotation.Columns.NOTE);
        sAnnotationProjectionMap.put(OnyxAnnotation.Columns.UPDATE_TIME, OnyxAnnotation.Columns.UPDATE_TIME);
        
        sThumbnailProjectionMap = new HashMap<String, String>();
        sThumbnailProjectionMap.put(OnyxThumbnail.Columns._ID, OnyxThumbnail.Columns._ID);
        sThumbnailProjectionMap.put(OnyxThumbnail.Columns._DATA, OnyxThumbnail.Columns._DATA);
        sThumbnailProjectionMap.put(OnyxThumbnail.Columns.SOURCE_MD5, OnyxThumbnail.Columns.SOURCE_MD5);
        sThumbnailProjectionMap.put(OnyxThumbnail.Columns.THUMBNAIL_KIND, OnyxThumbnail.Columns.THUMBNAIL_KIND);
        
        sHistoryProjectionMap = new HashMap<String, String>();
        sHistoryProjectionMap.put(OnyxHistoryEntry.Columns._ID, OnyxHistoryEntry.Columns._ID);
        sHistoryProjectionMap.put(OnyxHistoryEntry.Columns.MD5, OnyxHistoryEntry.Columns.MD5);
        sHistoryProjectionMap.put(OnyxHistoryEntry.Columns.START_TIME, OnyxHistoryEntry.Columns.START_TIME);
        sHistoryProjectionMap.put(OnyxHistoryEntry.Columns.END_TIME, OnyxHistoryEntry.Columns.END_TIME);
        sHistoryProjectionMap.put(OnyxHistoryEntry.Columns.PROGRESS, OnyxHistoryEntry.Columns.PROGRESS);
        sHistoryProjectionMap.put(OnyxHistoryEntry.Columns.EXTRA_ATTRIBUTES, OnyxHistoryEntry.Columns.EXTRA_ATTRIBUTES);
    }
    
    private static class DBHelper extends SQLiteOpenHelper {

        public DBHelper(Context context)
        {
            super(context, sDBName, null, sDBVersion);
        }
        
        @Override
        public void onCreate(SQLiteDatabase db)
        {
            Log.v(TAG, "onCreate");

            db.execSQL("CREATE TABLE " + OnyxLibraryItem.DB_TABLE_NAME + " ("
                    + OnyxLibraryItem.Columns._ID + " INTEGER PRIMARY KEY,"
                    + OnyxLibraryItem.Columns.PATH + " TEXT,"
                    + OnyxLibraryItem.Columns.NAME + " TEXT COLLATE NOCASE,"
                    + OnyxLibraryItem.Columns.SIZE + " LONG,"
                    + OnyxLibraryItem.Columns.TYPE + " TEXT,"
                    + OnyxLibraryItem.Columns.LAST_CHANGE + " LONG"
                    + ");");
            
            db.execSQL("CREATE TABLE " + OnyxMetadata.DB_TABLE_NAME + " ("
                    + OnyxMetadata.Columns._ID + " INTEGER PRIMARY KEY,"
                    + OnyxMetadata.Columns.MD5 + " TEXT,"
                    + OnyxMetadata.Columns.NAME + " TEXT,"
                    + OnyxMetadata.Columns.TITLE + " TEXT,"
                    + OnyxMetadata.Columns.AUTHORS + " TEXT,"
                    + OnyxMetadata.Columns.PUBLISHER + " TEXT,"
                    + OnyxMetadata.Columns.LANGUAGE + " TEXT,"
                    + OnyxMetadata.Columns.DESCRIPTION + " TEXT,"
                    + OnyxMetadata.Columns.LOCATION + " TEXT,"
                    + OnyxMetadata.Columns.NATIVE_ABSOLUTE_PATH + " TEXT,"
                    + OnyxMetadata.Columns.SIZE + " LONG,"
                    + OnyxMetadata.Columns.ENCODING + " TEXT,"
                    + OnyxMetadata.Columns.LAST_ACCESS + " LONG,"
                    + OnyxMetadata.Columns.LAST_MODIFIED + " LONG,"
                    + OnyxMetadata.Columns.PROGRESS + " TEXT,"
                    + OnyxMetadata.Columns.FAVORITE + " INT,"
                    + OnyxMetadata.Columns.RATING + " INT,"
                    + OnyxMetadata.Columns.TAGS + " TEXT,"
                    + OnyxMetadata.Columns.EXTRA_ATTRIBUTES + " TEXT"
                    + ");");
            
            db.execSQL("CREATE TABLE " + OnyxBookmark.DB_TABLE_NAME + " ("
                    + OnyxBookmark.Columns._ID + " INTEGER PRIMARY KEY,"
                    + OnyxBookmark.Columns.MD5 + " TEXT,"
                    + OnyxBookmark.Columns.QUOTE + " TEXT,"
                    + OnyxBookmark.Columns.LOCATION + " TEXT,"
                    + OnyxBookmark.Columns.UPDATE_TIME + " TEXT"
                    + ");");
            
            db.execSQL("CREATE TABLE " + OnyxAnnotation.DB_TABLE_NAME + " ("
                    + OnyxAnnotation.Columns._ID + " INTEGER PRIMARY KEY,"
                    + OnyxAnnotation.Columns.MD5 + " TEXT,"
                    + OnyxAnnotation.Columns.QUOTE + " TEXT,"
                    + OnyxAnnotation.Columns.LOCATION_BEGIN + " TEXT,"
                    + OnyxAnnotation.Columns.LOCATION_END + " TEXT,"
                    + OnyxAnnotation.Columns.NOTE + " TEXT,"
                    + OnyxAnnotation.Columns.UPDATE_TIME + " TEXT"
                    + ");");
            
            db.execSQL("CREATE TABLE " + OnyxThumbnail.DB_TABLE_NAME + " ("
                    + OnyxThumbnail.Columns._ID + " INTEGER PRIMARY KEY,"
                    + OnyxThumbnail.Columns._DATA + " TEXT,"
                    + OnyxThumbnail.Columns.SOURCE_MD5 + " TEXT,"
                    + OnyxThumbnail.Columns.THUMBNAIL_KIND + " TEXT"
                    + ");");
            
            db.execSQL("CREATE TABLE " + OnyxHistoryEntry.DB_TABLE_NAME + " ("
                    + OnyxHistoryEntry.Columns._ID + " INTEGER PRIMARY KEY,"
                    + OnyxHistoryEntry.Columns.MD5 + " TEXT,"
                    + OnyxHistoryEntry.Columns.START_TIME + " LONG,"
                    + OnyxHistoryEntry.Columns.END_TIME + " LONG,"
                    + OnyxHistoryEntry.Columns.PROGRESS + " TEXT,"
                    + OnyxHistoryEntry.Columns.EXTRA_ATTRIBUTES + " TEXT"
                    + ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion)
        {
            Log.v(TAG, "onUpgrade: " + oldVersion + " to " + newVersion);
            Log.w(TAG, "Upgrading database from version " + oldVersion + " to "
                    + newVersion + ", which will destroy all old data");
            // TODO: data is important, simply dropping is not be accepted 
//            db.execSQL("DROP TABLE IF EXISTS " + OnyxLibraryItem.DB_TABLE_NAME);
//            onCreate(db);
            if (newVersion == 3) {
            	db.execSQL("CREATE TABLE " + OnyxHistoryEntry.DB_TABLE_NAME + " ("
            			+ OnyxHistoryEntry.Columns._ID + " INTEGER PRIMARY KEY,"
            			+ OnyxHistoryEntry.Columns.MD5 + " TEXT,"
            			+ OnyxHistoryEntry.Columns.START_TIME + " LONG,"
            			+ OnyxHistoryEntry.Columns.END_TIME + " LONG,"
            			+ OnyxHistoryEntry.Columns.PROGRESS + " TEXT,"
            			+ OnyxHistoryEntry.Columns.EXTRA_ATTRIBUTES + " TEXT"
            			+ ");");
            }
        }
        
    }
    
    private DBHelper mDBHelper = null;
    
    public static String getThumbnailFile(Context context, String sourceMD5, String thumbnailKind)
    {
        String thumbnail_folder = ".thumbnails";
        String preferred_extension = ".jpg";
        String thumbnail_file = EnvironmentUtil.getExternalStorageAppCacheDirectory(context.getPackageName()) +
                File.separator + thumbnail_folder + File.separator + sourceMD5 + "." + thumbnailKind + preferred_extension;
        return thumbnail_file;
    }

    @Override
    public boolean onCreate()
    {
        mDBHelper = new DBHelper(this.getContext());
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder)
    {
        try {
            String order_by = sortOrder;
            SQLiteQueryBuilder builder = new SQLiteQueryBuilder(); 

            int matcher_result = sUriMatcher.match(uri);
            if ((matcher_result == MatcherResult.ITEMS) ||
                    (matcher_result == MatcherResult.ITEM_ID)) {
                builder.setTables(OnyxLibraryItem.DB_TABLE_NAME);
                builder.setProjectionMap(sItemProjectionMap);

                if (TextUtils.isEmpty(order_by)) {
                    order_by = OnyxLibraryItem.Columns.DEFAULT_ORDER_BY;
                }

                if (matcher_result == MatcherResult.ITEM_ID) {
                    builder.appendWhere(OnyxLibraryItem.Columns._ID + "=" + uri.getPathSegments().get(1));
                }
            }
            else if ((matcher_result == MatcherResult.METADATAS) ||
                    (matcher_result == MatcherResult.METADATA_ID)) {
                builder.setTables(OnyxMetadata.DB_TABLE_NAME);
                builder.setProjectionMap(sMetadataProjectionMap);

                if (matcher_result == MatcherResult.METADATA_ID) {
                    builder.appendWhere(OnyxMetadata.Columns._ID + "=" + uri.getPathSegments().get(1));
                }
            }
            else if ((matcher_result == MatcherResult.BOOKMARKS) || 
                    (matcher_result == MatcherResult.BOOKMARK_ID)) {
                builder.setTables(OnyxBookmark.DB_TABLE_NAME);
                builder.setProjectionMap(sBookmarkProjectionMap);

                if (matcher_result == MatcherResult.BOOKMARK_ID) {
                    builder.appendWhere(OnyxBookmark.Columns._ID + "=" + uri.getPathSegments().get(1));
                }
            }
            else if ((matcher_result == MatcherResult.ANNOTATIONS) || 
                    (matcher_result == MatcherResult.ANNOTATION_ID)) {
                builder.setTables(OnyxAnnotation.DB_TABLE_NAME);
                builder.setProjectionMap(sAnnotationProjectionMap);

                if (matcher_result == MatcherResult.ANNOTATION_ID) {
                    builder.appendWhere(OnyxAnnotation.Columns._ID + "=" + uri.getPathSegments().get(1));
                }
            }
            else if ((matcher_result == MatcherResult.THUMBNAILS) ||
                    (matcher_result == MatcherResult.THUMBNAIL_ID)) {
                builder.setTables(OnyxThumbnail.DB_TABLE_NAME);
                builder.setProjectionMap(sThumbnailProjectionMap);

                if (matcher_result == MatcherResult.THUMBNAIL_ID) {
                    builder.appendWhere(OnyxThumbnail.Columns._ID + "=" + uri.getPathSegments().get(1));
                }
            }
            else if (matcher_result == MatcherResult.HISTORIES || 
                    matcher_result == MatcherResult.HISTORY_ID) {
                builder.setTables(OnyxHistoryEntry.DB_TABLE_NAME);
                builder.setProjectionMap(sHistoryProjectionMap);
                
                if (matcher_result == MatcherResult.HISTORY_ID) {
                    builder.appendWhere(OnyxHistoryEntry.Columns._ID + "=" + uri.getPathSegments().get(1));
                }
            }
            else {
                Log.w(TAG, "Unknown URI: " + uri);
                return null;
            }

            SQLiteDatabase db = mDBHelper.getReadableDatabase();
            Cursor c = builder.query(db, projection, selection, selectionArgs, null, null, order_by);

            // Tell the cursor what uri to watch, so it knows when its source data changes
            c.setNotificationUri(getContext().getContentResolver(), uri);
            return c;
        }
        catch (Throwable tr) {
            Log.w(TAG, tr);
            return null;
        }
    }

    @Override
    public String getType(Uri uri)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public int bulkInsert(Uri uri, ContentValues[] values)
    {
        try {
            if (sUriMatcher.match(uri) != MatcherResult.ITEMS) {
                Log.w(TAG, "Unknown URI: " + uri);
                return 0;
            }

            for (ContentValues v : values) {
                if (!this.precheckLibraryitemValues(v)) {
                    return 0;
                }
            }

            SQLiteDatabase db  = null;
            try {
                db = mDBHelper.getWritableDatabase();
                db.beginTransaction();

                for (ContentValues v : values) {
                    long id = db.insert(OnyxLibraryItem.DB_TABLE_NAME, OnyxLibraryItem.Columns.NAME, v);
                    if (id < 0) {
                        return 0;
                    }
                }

                db.setTransactionSuccessful();

                getContext().getContentResolver().notifyChange(uri, null);
                return values.length;
            }
            finally {
                if (db != null) {
                    db.endTransaction();
                }
            }
        }
        catch (Throwable tr) {
            Log.w(TAG, tr);
            return 0;
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        try {
            String dst_table = null;
            String dst_null_column_hack = null;
            Uri dst_content_uri = null;

            final int match_result = sUriMatcher.match(uri);
            if (match_result == MatcherResult.ITEMS) {
                if (!this.precheckLibraryitemValues(values)) {
                    return null;
                }

                dst_table = OnyxLibraryItem.DB_TABLE_NAME;
                dst_null_column_hack = OnyxLibraryItem.Columns.NAME;
                dst_content_uri = OnyxLibraryItem.CONTENT_URI;
            }
            else if (match_result == MatcherResult.METADATAS) {
                dst_table = OnyxMetadata.DB_TABLE_NAME;
                dst_null_column_hack = OnyxMetadata.Columns.NAME;
                dst_content_uri = OnyxMetadata.CONTENT_URI;
            }
            else if (match_result == MatcherResult.BOOKMARKS) {
                dst_table = OnyxBookmark.DB_TABLE_NAME;
                dst_null_column_hack = OnyxBookmark.Columns.MD5;
                dst_content_uri = OnyxBookmark.CONTENT_URI;
            }
            else if (match_result == MatcherResult.ANNOTATIONS) {
                dst_table = OnyxAnnotation.DB_TABLE_NAME;
                dst_null_column_hack = OnyxAnnotation.Columns.MD5;
                dst_content_uri = OnyxAnnotation.CONTENT_URI;
            }
            else if (match_result == MatcherResult.THUMBNAILS) {
                dst_table = OnyxThumbnail.DB_TABLE_NAME;
                dst_null_column_hack = OnyxThumbnail.Columns.SOURCE_MD5;
                dst_content_uri = OnyxThumbnail.CONTENT_URI;

                String md5 = values.getAsString(OnyxThumbnail.Columns.SOURCE_MD5);
                String tk = values.getAsString(OnyxThumbnail.Columns.THUMBNAIL_KIND);
                String thumbnail_file = getThumbnailFile(this.getContext(), md5, tk);
                Log.d(TAG, "creating thumbnail file: " + thumbnail_file);
                if (!this.ensureFileExists(thumbnail_file)) {
                    Log.w(TAG, "Unable to create new file: " + thumbnail_file);
                    return null;
                }
                values.put(OnyxThumbnail.Columns._DATA, thumbnail_file);
            }
            else if (match_result == MatcherResult.HISTORIES) {
                dst_table = OnyxHistoryEntry.DB_TABLE_NAME;
                dst_null_column_hack = OnyxHistoryEntry.Columns.MD5;
                dst_content_uri = OnyxHistoryEntry.CONTENT_URI;
            }
            else {
                Log.w(TAG, "Unknown URI: " + uri);
                return null;
            }

            SQLiteDatabase db = mDBHelper.getWritableDatabase();
            long id = db.insert(dst_table, dst_null_column_hack, values);
            if (id < 0) {
                return null;
            }

            Uri ret = ContentUris.withAppendedId(dst_content_uri, id);
            this.getContext().getContentResolver().notifyChange(ret, null);

            return ret;
        }
        catch (Throwable tr) {
            Log.w(TAG, tr);
            return null;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs)
    {
        try {
            SQLiteDatabase db = mDBHelper.getWritableDatabase();
            int count = 0;
            
            switch (sUriMatcher.match(uri)) {
            case MatcherResult.ITEMS:
                count = db.delete(OnyxLibraryItem.DB_TABLE_NAME, selection, selectionArgs);
                break;
            case MatcherResult.ITEM_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxLibraryItem.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.delete(OnyxLibraryItem.DB_TABLE_NAME, where, selectionArgs);
                break;
            }
            case MatcherResult.METADATAS:
                count = db.delete(OnyxMetadata.DB_TABLE_NAME, selection, selectionArgs);
                break;
            case MatcherResult.METADATA_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxMetadata.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.delete(OnyxMetadata.DB_TABLE_NAME, where, selectionArgs);
                break;
            }
            case MatcherResult.BOOKMARKS:
                count = db.delete(OnyxBookmark.DB_TABLE_NAME, selection, selectionArgs);
                break;
            case MatcherResult.BOOKMARK_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxBookmark.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.delete(OnyxBookmark.DB_TABLE_NAME, where, selectionArgs);
                break;
            }
            case MatcherResult.ANNOTATIONS:
                count = db.delete(OnyxAnnotation.DB_TABLE_NAME, selection, selectionArgs);
                break;
            case MatcherResult.ANNOTATION_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxAnnotation.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.delete(OnyxAnnotation.DB_TABLE_NAME, where, selectionArgs);
                break;
            }
            case MatcherResult.THUMBNAILS:
                count = db.delete(OnyxThumbnail.DB_TABLE_NAME, selection, selectionArgs);
                break;
            case MatcherResult.THUMBNAIL_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxThumbnail.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.delete(OnyxThumbnail.DB_TABLE_NAME, where, selectionArgs);
                break;
            }
            case MatcherResult.HISTORIES:
                count = db.delete(OnyxHistoryEntry.DB_TABLE_NAME, selection, selectionArgs);
                break;
            case MatcherResult.HISTORY_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxHistoryEntry.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.delete(OnyxHistoryEntry.DB_TABLE_NAME, where, selectionArgs);
                break;
            }
            default:
                Log.w(TAG, "Unknown URI: " + uri); 
                return 0;
            }
            
            getContext().getContentResolver().notifyChange(uri, null);
            return count;
        }
        catch (Throwable tr) {
            Log.w(TAG, tr);
            return 0;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
            String[] selectionArgs)
    {
        try {
            SQLiteDatabase db = mDBHelper.getWritableDatabase();
            int count = 0;
            switch (sUriMatcher.match(uri)) {
            case MatcherResult.ITEMS:
                count = db.update(OnyxLibraryItem.DB_TABLE_NAME, values, selection, selectionArgs);
                break;
            case MatcherResult.ITEM_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxLibraryItem.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.update(OnyxLibraryItem.DB_TABLE_NAME, values, where, selectionArgs);
                break;
            }
            case MatcherResult.METADATAS:
                count = db.update(OnyxMetadata.DB_TABLE_NAME, values, selection, selectionArgs);
                break;
            case MatcherResult.METADATA_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxMetadata.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.update(OnyxMetadata.DB_TABLE_NAME, values, where, selectionArgs);
                break;
            }
            case MatcherResult.BOOKMARKS:
                count = db.update(OnyxBookmark.DB_TABLE_NAME, values, selection, selectionArgs);
                break;
            case MatcherResult.BOOKMARK_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxBookmark.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.update(OnyxBookmark.DB_TABLE_NAME, values, where, selectionArgs);
                break;
            }
            case MatcherResult.ANNOTATIONS:
                count = db.update(OnyxAnnotation.DB_TABLE_NAME, values, selection, selectionArgs);
                break;
            case MatcherResult.ANNOTATION_ID: {
                String id = uri.getPathSegments().get(1);
                String where = OnyxAnnotation.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.update(OnyxAnnotation.DB_TABLE_NAME, values, where, selectionArgs);
                break;
            }
            case MatcherResult.THUMBNAILS:
            case MatcherResult.THUMBNAIL_ID:
                assert(false);
                Log.w(TAG, "Thumbnail has no needs of updating");
                return 0;
            case MatcherResult.HISTORIES:
            	count = db.update(OnyxHistoryEntry.DB_TABLE_NAME, values, selection, selectionArgs);
            	break;
            case MatcherResult.HISTORY_ID:
            	String id = uri.getPathSegments().get(1);
                String where = OnyxHistoryEntry.Columns._ID + "=" + id;
                if (!TextUtils.isEmpty(selection)) {
                    where = where + " AND (" + selection + ")";
                }
                count = db.update(OnyxHistoryEntry.DB_TABLE_NAME, values, where, selectionArgs);
                break;
            default:
                Log.w(TAG, "Unknown URI: " + uri); 
                return 0;
            }
            
            getContext().getContentResolver().notifyChange(uri, null);
            return count;
        }
        catch (Throwable tr) {
            Log.w(TAG, tr);
            return 0;
        }
    }
    
    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode)
            throws FileNotFoundException
    {
        int matcher_result = sUriMatcher.match(uri);
        if (matcher_result == MatcherResult.THUMBNAIL_ID) {
            ParcelFileDescriptor d = this.openFileHelper(uri, mode);
            return d;
        }
        else {
            assert(false);
            return null;
        }
    }
    
    private boolean precheckLibraryitemValues(ContentValues values) 
    {
        if (!values.containsKey(OnyxLibraryItem.Columns.PATH)) {
            Log.w(TAG, "missing value: path");
            return false;
        }
        if (!values.containsKey(OnyxLibraryItem.Columns.NAME)) {
            Log.w(TAG, "missing value: name");
            return false;
        }
        
        return true;
    }
    
    private boolean ensureFileExists(String path) {
        File file = new File(path);
        if (file.exists()) {
            return true;
        } 
        else {
            // we will not attempt to create the first directory in the path
            // (for example, do not create /sdcard if the SD card is not mounted)
            int secondSlash = path.indexOf('/', 1);
            if (secondSlash < 1) return false;
            String directoryPath = path.substring(0, secondSlash);
            File directory = new File(directoryPath);
            if (!directory.exists()) {
                return false;
            }
            
            File parent_folder = file.getParentFile();
            if (!parent_folder.exists() && !parent_folder.mkdirs()) {
                Log.e(TAG, "create folder failed: " + parent_folder.getAbsolutePath());
                return false;
            }
            try {
                return file.createNewFile();
            } catch(IOException ioe) {
                Log.e(TAG, "File creation failed", ioe);
            }
            return false;
        }
    }

}
