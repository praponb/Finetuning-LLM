from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy("played_at", "id")

valid_plays = df_plays

latest_per_key = (
    valid_plays
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

annotated = (
    latest_per_key
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

with_parent_listener = (
    annotated.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.signup_date") >= F.lit("2024-04-16").cast("date"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.days_since_played_at",
        "s.played_at_label",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

listener_id_stats = (
    with_parent_listener
    .groupBy("listener_id")
    .agg(F.avg("days_since_played_at").alias("avg_days_since_played_at"))
)

outliers = (
    with_parent_listener.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.days_since_played_at") > F.col("g.avg_days_since_played_at"))
    .select(
        "s.*",
        "g.avg_days_since_played_at",
        F.round(F.col("s.days_since_played_at") - F.col("g.avg_days_since_played_at"), 2).alias("days_since_played_at_minus_avg"),
    )
)

joined_listener = (
    outliers.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.days_since_played_at",
        "s.played_at_label",
        "s.listener_signup_date",
        "s.avg_days_since_played_at",
        "s.days_since_played_at_minus_avg",
        F.col("l.interests").alias("listener_interests"),
    )
)

rollup_base = (
    joined_listener
    .groupBy(
        F.date_trunc("month", "listener_signup_date").alias("listener_signup_date_month"),
    )
    .agg(
        F.count(F.when(F.col("source") == "playlist", 1)).alias("n_source_playlist"),
        F.round(F.sum("days_since_played_at") / F.nullif(F.sum("avg_days_since_played_at"), F.lit(0)), 3).alias("days_since_played_at_to_avg_days_since_played_at"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("avg_days_since_played_at") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("avg_days_since_played_at_to_ms_played_ratio"),
        F.min("days_since_played_at_minus_avg").alias("min_days_since_played_at_minus_avg"),
    )
)

result = (
    rollup_base
    .filter(F.col("n_source_playlist") <= 6)
    .select(
        "listener_signup_date_month",
        "avg_days_since_played_at_to_ms_played_ratio",
        "min_days_since_played_at_minus_avg",
        F.when(F.col("n_source_playlist") >= 6, "large").when(F.col("n_source_playlist") >= 4, "medium").otherwise("small").alias("n_source_playlist_tier"),
    )
    .orderBy(
        "avg_days_since_played_at_to_ms_played_ratio",
        F.col("listener_signup_date_month").asc(),
    )
    .limit(5)
)
