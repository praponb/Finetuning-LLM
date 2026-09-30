from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("ms_played").between(112786, 225733))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

with_details_listener = (
    scoped_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source_slug",
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    with_details_listener
    .cube("device", "track_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "track_id",
        "gid",
        "device_list",
        "n_rows",
        "distinct_ids",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("track_id").asc_nulls_last())
)
