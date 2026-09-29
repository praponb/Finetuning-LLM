from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.col("ms_played") >= 49807) | (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("radio", "search"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

with_parent = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

derived = (
    with_parent
    .select(
        "*",
        F.round(F.col("is_skipped_flag") / F.nullif(F.col("listener_birth_year"), F.lit(0)), 3).alias("is_skipped_flag_per_listener_birth_year"),
    )
)

result = (
    derived
    .groupBy("listener_id")
    .pivot("source", ["search", "playlist", "album"])
    .agg(F.sum("ms_played"))
    .orderBy("listener_id")
)
