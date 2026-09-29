from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
    )
)

with_parent = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
    )
)

annotated = (
    with_parent
    .select(
        "*",
        F.month(F.col("played_at")).alias("played_at_month"),
        F.concat(F.col("listener_plan"), F.lit("#"), F.col("track_id").cast("string")).alias("listener_plan_ref"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

labeled = (
    annotated
    .filter(F.col("is_skipped"))
    .select(
        "*",
        F.when(F.col("listener_plan").isin("premium", "student"), "group_a").when(F.col("listener_plan") == "family", "group_b").otherwise("other").alias("listener_plan_group"),
    )
)

result = (
    labeled
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "listener_interests",
        "played_at_month",
        "listener_plan_ref",
        "is_skipped_flag",
        "listener_plan_group",
        F.abs(F.col("played_at_month") - 6).alias("played_at_month_distance"),
        F.upper(F.trim(F.col("listener_plan_ref"))).alias("listener_plan_ref_clean"),
    )
    .orderBy("listener_id", F.col("listener_plan_ref_clean").asc())
)
