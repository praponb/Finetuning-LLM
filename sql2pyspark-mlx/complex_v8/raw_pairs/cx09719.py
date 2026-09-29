from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("source").isin("search", "album", "radio")) & (F.col("listener_id") <= 6))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 245815, "HIGH").when(F.col("ms_played") >= 112786, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isin("radio", "playlist", "search")) & (~F.col("p.is_skipped")), "left_anti")
)

filtered = (
    with_activity
    .crossJoin(with_activity.agg(F.min("ms_played").alias("overall_min_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_min_ms_played"))
    .drop("overall_min_ms_played")
)

derived = (
    filtered
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("ms_played_tier")), " ", "_")).alias("ms_played_tier_slug"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier_slug",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

per_label = (
    combined_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "ms_played_tier_slug",
        "listener_plan",
        F.explode("listener_interests").alias("listener_interest"),
    )
)

result = (
    per_label
    .select(
        "id",
        "track_id",
        "is_skipped",
        "listener_plan",
        F.concat(F.col("listener_plan"), F.lit("#"), F.col("track_id").cast("string")).alias("listener_plan_ref"),
    )
    .orderBy("track_id")
)
