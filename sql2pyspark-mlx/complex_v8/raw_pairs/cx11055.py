from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_source_not_in_alb = Window.orderBy(F.col("pct_source_not_in_album_playlist").asc_nulls_last(), "track_id", "device", "played_at_month")
w_all_by_pct_source_not_in_alb_2 = Window.orderBy(F.col("pct_source_not_in_album_playlist").asc_nulls_last())
w_min_is_skipped_flag = Window.partitionBy("min_is_skipped_flag")

scoped = (
    df_plays
    .filter(F.col("track_id") >= 19)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

with_activity = (
    scoped
    .crossJoin(scoped.agg(F.avg("ms_played").alias("overall_avg_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_avg_ms_played"))
    .drop("overall_avg_ms_played")
)

labeled = (
    with_activity
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_bucket",
        "is_skipped_flag",
        F.abs(F.col("is_skipped_flag") - 1).alias("is_skipped_flag_distance"),
    )
)

summary = (
    labeled
    .filter(F.col("is_skipped"))
    .groupBy(
        "track_id",
        "device",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("device").isin("phone", "tv"), 1)) / F.count("*"), 1).alias("pct_device_in_phone_tv"),
        F.round(100 * F.count(F.when(~F.col("source").isin("album", "playlist"), 1)) / F.count("*"), 1).alias("pct_source_not_in_album_playlist"),
        F.min("is_skipped_flag").alias("min_is_skipped_flag"),
    )
)

screened = (
    summary.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.source").isin("search", "album")) & (F.hour(F.col("p.played_at")).between(10, 15)), "left_anti")
)

windowed = (
    screened
    .select(
        "track_id",
        "device",
        "played_at_month",
        "pct_source_not_in_album_playlist",
        F.dense_rank().over(w_all_by_pct_source_not_in_alb).alias("pct_source_not_in_album_playlist_dense_rank"),
        F.dense_rank().over(w_all_by_pct_source_not_in_alb_2).alias("pct_source_not_in_album_playlist_dense_rank_2"),
        F.size(F.collect_set("track_id").over(w_min_is_skipped_flag)).alias("distinct_track_id_in_min_is_skipped_flag"),
        F.round(F.avg("min_is_skipped_flag").over(w_min_is_skipped_flag), 2).alias("min_is_skipped_flag_avg_min_is_skipped_flag"),
        F.count(F.lit(1)).over(w_min_is_skipped_flag).alias("rows_in_min_is_skipped_flag"),
    )
)

result = (
    windowed
    .filter(F.to_date(F.col("played_at_month")) < F.lit("2025-02-01").cast("date"))
    .select(
        "track_id",
        "device",
        "played_at_month",
        "pct_source_not_in_album_playlist_dense_rank",
        "pct_source_not_in_album_playlist_dense_rank_2",
        "distinct_track_id_in_min_is_skipped_flag",
        "rows_in_min_is_skipped_flag",
        F.to_date(F.col("played_at_month")).alias("played_at_month_date"),
        F.date_trunc("hour", F.col("played_at_month")).alias("played_at_month_hour"),
    )
    .orderBy(F.col("device").desc())
)
