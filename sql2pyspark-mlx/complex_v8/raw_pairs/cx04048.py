from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_device_group_played_at_month = Window.partitionBy("device_group", F.date_trunc("month", "played_at")).orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id")
w_device_group_played_at_month_2 = Window.partitionBy("device_group", F.date_trunc("month", "played_at")).orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_device_group_played_at_month_3 = Window.partitionBy("device_group", F.date_trunc("month", "played_at")).orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter((F.col("track_id") >= 3) & (F.col("ms_played") >= 51883))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.when(F.col("device").isin("car", "phone"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "source_clean",
        "ms_played_bucket",
        "device_group",
        F.min("ms_played_bucket").over(w_track_id).alias("track_id_min_ms_played_bucket"),
        F.ntile(4).over(w_device_group_played_at_month).alias("ms_played_bucket_quartile"),
        F.round(F.avg("ms_played").over(w_device_group_played_at_month_2), 2).alias("ms_played_avg_3"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.size(F.collect_set("device_group").over(w_track_id)).alias("distinct_device_group_in_track_id"),
        F.last("ms_played_bucket", ignorenulls=True).over(w_device_group_played_at_month_3).alias("last_known_ms_played_bucket"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        "s.source_clean",
        "s.ms_played_bucket",
        "s.track_id_min_ms_played_bucket",
        "s.ms_played_avg_3",
        "s.ms_played_vs_track_id_avg",
        "s.distinct_device_group_in_track_id",
        "s.last_known_ms_played_bucket",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

result = (
    with_parent
    .cube("track_id", "track_is_explicit")
    .agg(
        F.grouping("track_id").alias("is_total_track_id"),
        F.max("track_release_date").alias("last_track_release_date"),
        F.min("last_known_ms_played_bucket").alias("min_last_known_ms_played_bucket"),
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("track_is_explicit").asc_nulls_last(),
    )
)
