from pyspark.sql import functions as F

cleaned = (
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
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

stats = (
    cleaned
    .groupBy("listener_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("ms_played_bucket"), 2).alias("stddev_ms_played_bucket"),
        F.count("*").alias("n_rows"),
        F.max("played_at").alias("last_played_at"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_parent_listener = (
    stats.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.listener_id",
        "s.stddev_ms_played_bucket",
        "s.n_rows",
        "s.last_played_at",
        "s.first_played_at",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

combined_details = (
    with_parent_listener.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    combined_details
    .select(
        "listener_id",
        "stddev_ms_played_bucket",
        "last_played_at",
        "first_played_at",
        "listener_plan",
        "listener_interests",
        "listener_signup_date",
        "listener_birth_year",
        F.round(F.col("stddev_ms_played_bucket") * 1.5, 2).alias("stddev_ms_played_bucket_adj"),
        F.substring(F.trim(F.col("listener_plan")), 1, 3).alias("listener_plan_prefix"),
    )
    .orderBy(F.col("listener_plan_prefix").asc())
)
