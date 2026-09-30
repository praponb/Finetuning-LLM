from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")
w_track_id_played_at_month_by = Window.partitionBy("track_id", F.month("played_at")).orderBy(F.col("played_at").asc_nulls_last())
w_all_by_played_at_percent_ran = Window.orderBy(F.col("played_at_percent_rank").desc())
w_track_id = Window.partitionBy("track_id")
w_all_by_played_at_percent_ran_2 = Window.orderBy(F.col("played_at_percent_rank").desc(), "id")

filtered_plays = (
    df_plays
    .filter((F.col("device").isin("desktop", "phone", "tv")) & (F.col("listener_id") <= 4))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 166517).alias("ms_played_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.abs(F.col("ms_played") - 107912).alias("ms_played_distance_2"),
    )
)

screened = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") <= 246779), "left_semi")
    .filter(F.col("listener_id") <= 10)
)

sequenced = (
    screened
    .withColumn("is_skipped_listener_id_max_ms_played_distance_2", F.max("ms_played_distance_2").over(w_is_skipped_listener_id))
    .withColumn("played_at_percent_rank", F.round(F.percent_rank().over(w_track_id_played_at_month_by), 3))
)

device_stats = (
    sequenced
    .groupBy("device")
    .agg(F.percentile_approx("days_since_played_at", 0.5).alias("median_days_since_played_at"))
)

outliers = (
    sequenced.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.days_since_played_at") >= F.col("g.median_days_since_played_at"))
    .select(
        "s.*",
        "g.median_days_since_played_at",
        F.round(F.col("s.days_since_played_at") - F.col("g.median_days_since_played_at"), 2).alias("days_since_played_at_minus_median"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_played_at_percent_ran), 3).alias("played_at_percent_rank_percent_rank"),
        F.round(F.col("ms_played_distance") - F.avg("ms_played_distance").over(w_track_id), 2).alias("ms_played_distance_vs_track_id_avg"),
        F.dense_rank().over(w_all_by_played_at_percent_ran).alias("played_at_percent_rank_dense_rank"),
        (F.col("median_days_since_played_at") - F.lag("median_days_since_played_at").over(w_all_by_played_at_percent_ran_2)).alias("median_days_since_played_at_change"),
    )
)

result = (
    metrics
    .filter(F.col("days_since_played_at") >= 105)
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "source",
        "ms_played_distance",
        "days_since_played_at",
        "is_skipped_listener_id_max_ms_played_distance_2",
        "median_days_since_played_at",
        "days_since_played_at_minus_median",
        "played_at_percent_rank_percent_rank",
        "played_at_percent_rank_dense_rank",
        F.when(F.col("device").isin("car", "speaker"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.round(F.col("played_at_percent_rank") * 0.25, 2).alias("played_at_percent_rank_adj"),
    )
    .orderBy(
        F.col("is_skipped_listener_id_max_ms_played_distance_2").desc(),
        F.col("listener_id").desc(),
        "id",
    )
    .limit(5)
)
