from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(F.col("track_id") >= 5)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

derived = (
    recent
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        F.when(F.col("ms_played") >= 245815, "A").when(F.col("ms_played") >= 129336, "B").otherwise("C").alias("ms_played_tier"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.ms_played",
        "s.device",
        "s.ms_played_tier",
        "s.device_clean",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "ms_played",
        "device",
        "device_clean",
        "listener_plan",
        "listener_interests",
        "listener_username",
        F.upper(F.trim(F.col("listener_plan"))).alias("listener_plan_clean"),
        F.when(F.col("device").isin("tv", "desktop"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
)
