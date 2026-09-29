from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_listener_id_source_by_days_s = Window.partitionBy("listener_id", "source").orderBy(F.col("days_since_played_at").desc())
w_source_is_skipped = Window.partitionBy("source", "is_skipped")
w_device_by_days_since_played = Window.partitionBy("device").orderBy(F.col("days_since_played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_played_at = Window.orderBy(F.col("days_since_played_at").asc_nulls_last(), "id")
w_is_skipped_by_days_since_pla = Window.partitionBy("is_skipped").orderBy("days_since_played_at")
w_listener_id_track_id_by_days = Window.partitionBy("listener_id", "track_id").orderBy("days_since_played_at", "id")
w_device_by_days_since_played_2 = Window.partitionBy("device").orderBy(F.col("days_since_played_at").desc_nulls_first(), "id")
w_device_by_days_since_played_3 = Window.partitionBy("device").orderBy(F.col("days_since_played_at").desc_nulls_first())

prepared_plays = (
    df_plays
    .filter(F.col("ms_played") > 56848)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_trends = (
    prepared_plays
    .select(
        "*",
        F.sum("days_since_played_at").over(w_source).alias("source_sum_days_since_played_at"),
        F.round(F.cume_dist().over(w_listener_id_source_by_days_s), 3).alias("days_since_played_at_cume_dist"),
        F.min("days_since_played_at").over(w_source_is_skipped).alias("source_is_skipped_min_days_since_played_at"),
        F.size(F.collect_set("device").over(w_source_is_skipped)).alias("distinct_device_in_source_is_skipped"),
        F.last("days_since_played_at", ignorenulls=True).over(w_device_by_days_since_played).alias("last_known_days_since_played_at"),
        F.round(F.cume_dist().over(w_all_by_days_since_played_at), 3).alias("days_since_played_at_cume_dist_2"),
        F.dense_rank().over(w_is_skipped_by_days_since_pla).alias("days_since_played_at_dense_rank"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_source), 2).alias("days_since_played_at_vs_source_avg"),
        F.round(F.avg("days_since_played_at").over(w_source), 2).alias("source_avg_days_since_played_at"),
        (F.col("days_since_played_at") - F.lag("days_since_played_at").over(w_listener_id_track_id_by_days)).alias("days_since_played_at_change"),
        (F.col("days_since_played_at") - F.lag("days_since_played_at").over(w_device_by_days_since_played_2)).alias("days_since_played_at_change_2"),
        F.rank().over(w_device_by_days_since_played_3).alias("days_since_played_at_rank"),
    )
)

stats = (
    with_trends
    .groupBy("source", "track_id", "is_skipped")
    .agg(
        F.min("source_sum_days_since_played_at").alias("min_source_sum_days_since_played_at"),
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("n_source_is_not_null"),
        F.round(F.sum("days_since_played_at_cume_dist_2"), 2).alias("total_days_since_played_at_cume_dist_2"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select(
        "source",
        "track_id",
        "is_skipped",
        "min_source_sum_days_since_played_at",
        "total_days_since_played_at_cume_dist_2",
        "cnt",
    )
    .orderBy("min_source_sum_days_since_played_at", F.col("source").desc())
)
