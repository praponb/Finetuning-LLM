from pyspark.sql import functions as F

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    enriched
    .groupingSets(
        [["device", "is_skipped"], ["device"], ["is_skipped"], []],
        "device", "is_skipped",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "is_skipped",
        "grouping_level",
        "device_list",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("is_skipped").asc_nulls_last())
)
