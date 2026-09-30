from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

with_parent_listener = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.birth_year") < 1994)
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_parent_listener
    .cube("source", "listener_id")
    .agg(
        F.array_sort(F.collect_set("listener_plan")).alias("listener_plan_list"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "listener_id",
        "listener_plan_list",
    )
    .orderBy(F.col("source").asc_nulls_last(), F.col("listener_id").asc_nulls_last())
)
