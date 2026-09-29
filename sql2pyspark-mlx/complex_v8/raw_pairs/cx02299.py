from pyspark.sql import functions as F

filtered_listeners = (
    df_listeners
    .filter((F.col("birth_year") > 1967) & (F.col("plan") == "premium"))
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
    )
)

labeled = (
    filtered_listeners
    .filter(F.col("username").isNotNull())
    .select(
        "*",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
        F.length(F.trim(F.col("plan"))).alias("plan_len"),
        F.substring(F.trim(F.col("plan")), 1, 3).alias("plan_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"))
)

with_stats = (
    labeled.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_activity = (
    with_stats
    .crossJoin(with_stats.agg(F.min("play_count").alias("overall_min_play_count")))
    .filter(F.col("play_count") > F.col("overall_min_play_count"))
    .drop("overall_min_play_count")
)

with_children = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.username",
        "s.plan",
        "s.birth_year",
        "s.birth_year_bucket",
        "s.plan_prefix",
        "s.play_count",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

scored = (
    with_children
    .filter(F.col("play_played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        "birth_year_bucket",
        "play_id",
        "play_played_at",
        "play_device",
        F.upper(F.trim(F.col("play_device"))).alias("play_device_clean"),
        F.greatest(F.col("birth_year"), F.col("birth_year_bucket")).alias("greatest_birth_year_birth_year_bucket"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.hour(F.col("p.played_at")).between(7, 12)) & (F.col("p.is_skipped")), "left_semi")
)

result = (
    without_matches
    .cube("play_id", "plan", "play_device")
    .agg(
        F.sum(F.when(F.col("play_device") == "phone", 1).otherwise(0)).alias("n_play_device_phone"),
        F.sum(F.when(F.col("plan").isin("free", "premium", "family"), F.col("birth_year_bucket"))).alias("birth_year_bucket_plan_in_free_premium_family"),
    )
)
