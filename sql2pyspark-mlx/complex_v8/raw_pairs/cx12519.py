from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_played_at_ties = Window.partitionBy("device").orderBy(F.col("played_at").desc())
w_track_id_played_at_month_by = Window.partitionBy("track_id", F.date_trunc("month", "played_at")).orderBy(F.col("played_at").asc_nulls_last(), "id")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_played_at_to_date_by_ms_play = Window.partitionBy(F.to_date("played_at")).orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_to_date_by_ms_play_2 = Window.partitionBy(F.to_date("played_at")).orderBy(F.col("ms_played").desc(), "id")
w_track_id_played_at_month_by_2 = Window.partitionBy("track_id", F.date_trunc("month", "played_at")).orderBy(F.col("played_at").asc_nulls_last())

prepared_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

ranked = (
    prepared_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "device_slug",
        F.round(F.cume_dist().over(w_device_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.lag("ms_played", 3).over(w_track_id_played_at_month_by).alias("prev3_ms_played"),
        F.dense_rank().over(w_device_by_played_at_ties).alias("played_at_dense_rank"),
        F.count(F.lit(1)).over(w_is_skipped_track_id).alias("rows_in_is_skipped_track_id"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped_track_id), F.lit(0)), 4).alias("ms_played_share_of_is_skipped_track_id"),
        F.round(F.avg("ms_played").over(w_played_at_to_date_by_ms_play), 2).alias("ms_played_avg_7"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_track_id_by_ms_p).alias("last_known_ms_played"),
        F.size(F.collect_set("source").over(w_is_skipped_track_id)).alias("distinct_source_in_is_skipped_track_id"),
        (F.col("ms_played") - F.lag("ms_played").over(w_played_at_to_date_by_ms_play_2)).alias("ms_played_change"),
        F.dense_rank().over(w_track_id_played_at_month_by_2).alias("played_at_dense_rank_2"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("played_at_dense_rank") <= 2)
    .drop("played_at_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "device_slug",
        "played_at_cume_dist",
        "rows_in_is_skipped_track_id",
        "last_known_ms_played",
        "distinct_source_in_is_skipped_track_id",
        "ms_played_change",
        "played_at_dense_rank_2",
        F.when(F.col("played_at_dense_rank_2") >= 8, "HIGH").when(F.col("played_at_dense_rank_2") >= 4, "MEDIUM").otherwise("LOW").alias("played_at_dense_rank_2_tier"),
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
    .orderBy(F.col("rows_in_is_skipped_track_id").desc_nulls_first())
)
