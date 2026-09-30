from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_played_at_month_by_play_sour = Window.partitionBy(F.month("played_at")).orderBy(F.col("play_source_count").asc_nulls_last(), "id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")

filtered_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")) | (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_counts = (
    filtered_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("source").isin("radio", "album"), 1)).alias("play_source_count"),
    )
)

with_stats = (
    with_counts.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

enriched = (
    with_stats
    .filter(F.col("days_since_played_at") <= 630)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "device_source_label",
        "days_since_played_at",
        "play_source_count",
        F.lower(F.regexp_replace(F.trim(F.col("device_source_label")), " ", "_")).alias("device_source_label_slug"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("ms_played").over(w_is_skipped).alias("is_skipped_sum_ms_played"),
        F.round(F.avg("play_source_count").over(w_is_skipped), 2).alias("is_skipped_avg_play_source_count"),
        F.min("days_since_played_at").over(w_is_skipped).alias("is_skipped_min_days_since_played_at"),
        F.lead("played_at").over(w_played_at_month_by_play_sour).alias("next_played_at"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "device_source_label",
        "days_since_played_at",
        "device_source_label_slug",
        "is_skipped_sum_ms_played",
        "is_skipped_avg_play_source_count",
        "is_skipped_min_days_since_played_at",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.date_trunc("month", F.col("next_played_at")).alias("next_played_at_month"),
    )
    .orderBy(
        F.col("ms_played_filled").asc_nulls_last(),
        "is_skipped_avg_play_source_count",
        "id",
    )
    .limit(5)
)
