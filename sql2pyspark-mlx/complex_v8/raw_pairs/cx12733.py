from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_track_id_by_ms_played_bucket = Window.partitionBy("track_id").orderBy(F.col("ms_played_bucket").asc_nulls_last(), "id")
w_track_id_listener_id_by_ms_p = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, Window.currentRow)
w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_listener_id_by_ms_pla_2 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_track_id_by_ms_played_bucket_2 = Window.partitionBy("track_id").orderBy(F.col("ms_played_bucket").asc_nulls_last())
w_distinct_listener_id_in_sour = Window.partitionBy("distinct_listener_id_in_source", "device")
w_all_by_source_max_ms_played = Window.orderBy("source_max_ms_played_bucket", "id")
w_track_id_distinct_listener_i = Window.partitionBy("track_id", "distinct_listener_id_in_source").orderBy(F.col("source_max_ms_played_bucket").desc(), "id")

filtered = (
    df_plays
    .filter((F.col("ms_played").between(62294, 308104)) & (F.hour(F.col("played_at")).between(8, 14)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
        F.round(F.cume_dist().over(w_track_id_by_ms_played_bucket), 3).alias("ms_played_bucket_cume_dist"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.round(F.avg("ms_played_bucket").over(w_track_id_listener_id_by_ms_p), 2).alias("ms_played_bucket_avg_3"),
        F.max("ms_played_bucket").over(w_source).alias("source_max_ms_played_bucket"),
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.sum("ms_played").over(w_source).alias("source_sum_ms_played"),
        F.last("ms_played_bucket").over(w_device_listener_id_by_ms_pla).alias("last_ms_played_bucket"),
        F.lag("ms_played_bucket").over(w_device_listener_id_by_ms_pla_2).alias("prev_ms_played_bucket"),
        F.round(F.percent_rank().over(w_track_id_by_ms_played_bucket_2), 3).alias("ms_played_bucket_percent_rank"),
        F.ntile(10).over(w_device_listener_id_by_ms_pla_2).alias("ms_played_decile"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.col("source_sum_ms_played") / F.nullif(F.sum("source_sum_ms_played").over(w_distinct_listener_id_in_sour), F.lit(0)), 4).alias("source_sum_ms_played_share_of_distinct_listener_"),
        F.ntile(5).over(w_all_by_source_max_ms_played).alias("source_max_ms_played_bucket_quintile"),
        F.round(F.cume_dist().over(w_track_id_distinct_listener_i), 3).alias("source_max_ms_played_bucket_cume_dist"),
        F.sum("ms_played_bucket_cume_dist").over(w_distinct_listener_id_in_sour).alias("distinct_listener_id_in_source_device_sum_ms_pla"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "track_id",
        "ms_played",
        "source",
        "ms_played_bucket",
        "source_max_ms_played_bucket",
        "source_sum_ms_played",
        "prev_ms_played_bucket",
        "ms_played_bucket_percent_rank",
        "ms_played_decile",
        "source_sum_ms_played_share_of_distinct_listener_",
        "source_max_ms_played_bucket_quintile",
        "source_max_ms_played_bucket_cume_dist",
        "distinct_listener_id_in_source_device_sum_ms_pla",
        F.coalesce(F.col("source_sum_ms_played"), F.lit(0)).alias("source_sum_ms_played_filled"),
    )
    .orderBy(F.col("source").desc_nulls_first())
)
