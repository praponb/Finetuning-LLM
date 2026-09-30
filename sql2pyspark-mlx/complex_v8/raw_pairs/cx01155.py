from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id = Window.partitionBy("source", "listener_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_track_id_by_played_at_year = Window.partitionBy("track_id").orderBy("played_at_year", "id")
w_source_by_played_at_year = Window.partitionBy("source").orderBy(F.col("played_at_year").desc(), "id")
w_track_id_by_played_at_year_f = Window.partitionBy("track_id").orderBy("played_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_played_at_year_tie = Window.partitionBy("source").orderBy(F.col("played_at_year").desc())
w_all_by_ms_played_moving_1 = Window.orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)

cleaned = (
    df_plays
    .filter((F.col("track_id") <= 13) & (F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source_listener_id), 2).alias("ms_played_vs_source_listener_id_avg"),
        F.ntile(10).over(w_all_by_ms_played).alias("ms_played_decile"),
        F.lag("played_at", 2).over(w_track_id_by_played_at_year).alias("prev2_played_at"),
        F.row_number().over(w_all_by_ms_played).alias("rn"),
        F.round(F.cume_dist().over(w_source_by_played_at_year), 3).alias("played_at_year_cume_dist"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_source_listener_id), F.lit(0)), 4).alias("ms_played_bucket_share_of_source_listener_id"),
        F.last("played_at_year").over(w_track_id_by_played_at_year_f).alias("last_played_at_year"),
        F.row_number().over(w_track_id_by_played_at_year).alias("seq_no"),
        F.round(F.percent_rank().over(w_source_by_played_at_year_tie), 3).alias("played_at_year_percent_rank"),
        F.row_number().over(w_source_by_played_at_year).alias("rn_2"),
        F.round(F.avg("ms_played").over(w_source_listener_id), 2).alias("source_listener_id_avg_ms_played"),
        F.lead("ms_played_bucket", 3).over(w_all_by_ms_played).alias("next3_ms_played_bucket"),
        F.round(F.avg("ms_played_bucket").over(w_all_by_ms_played_moving_1), 2).alias("ms_played_bucket_avg_2"),
        F.min("ms_played").over(w_source_listener_id).alias("source_listener_id_min_ms_played"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "ms_played_bucket",
        "ms_played_vs_source_listener_id_avg",
        "prev2_played_at",
        "rn",
        "played_at_year_cume_dist",
        "last_played_at_year",
        "seq_no",
        "played_at_year_percent_rank",
        "rn_2",
        "source_listener_id_avg_ms_played",
        "ms_played_bucket_avg_2",
        "source_listener_id_min_ms_played",
        F.date_format(F.col("last_played_at_year"), "yyyy-MM-dd").alias("last_played_at_year_label"),
        F.round(F.col("played_at_year_cume_dist") / F.nullif(F.col("ms_played_bucket"), F.lit(0)), 3).alias("played_at_year_cume_dist_per_ms_played_bucket"),
        (F.col("next3_ms_played_bucket") - F.col("played_at_year_cume_dist")).alias("next3_ms_played_bucket_minus_played_at_year_cume"),
    )
)

stats = (
    scored
    .groupBy("rn", "seq_no", "device")
    .agg(
        F.max("source_listener_id_avg_ms_played").alias("max_source_listener_id_avg_ms_played"),
        F.round(F.sum("rn") / F.nullif(F.sum("played_at_year_percent_rank"), F.lit(0)), 3).alias("rn_to_played_at_year_percent_rank_ratio"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    stats
    .filter(F.col("rn") <= 9)
    .select(
        "rn",
        "device",
        "max_source_listener_id_avg_ms_played",
        "rn_to_played_at_year_percent_rank_ratio",
        "cnt",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
    )
    .orderBy("max_source_listener_id_avg_ms_played")
)
