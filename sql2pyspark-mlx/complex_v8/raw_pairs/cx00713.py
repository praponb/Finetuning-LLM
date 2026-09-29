from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.col("ms_played") >= 108979)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 308104, "A").when(F.col("ms_played") >= 73335, "B").otherwise("C").alias("ms_played_tier"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.size(F.col("l.interests")) >= 1)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.interests").alias("listener_interests"),
    )
)

scored = (
    with_details
    .select(
        "*",
        F.concat_ws("|", F.col("ms_played_tier"), F.col("device")).alias("ms_played_tier_device_label"),
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.when(F.col("ms_played_tier").isin("C", "A"), "group_a").when(F.col("ms_played_tier") == "B", "group_b").otherwise("other").alias("ms_played_tier_group"),
    )
)

result = (
    scored
    .groupingSets(
        [["device", "ms_played_tier_group"], ["device"], []],
        "device", "ms_played_tier_group",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
    )
)
