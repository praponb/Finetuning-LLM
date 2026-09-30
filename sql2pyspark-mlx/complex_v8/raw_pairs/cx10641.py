from pyspark.sql import functions as F

base_plays = df_plays.filter((F.col("listener_id") >= 3) & (F.col("device") == "tv"))

detail_rows = (
    base_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["is_skipped", "play_listener_id"], ["is_skipped"], []],
        "is_skipped", "play_listener_id",
    )
    .agg(
        F.grouping("is_skipped").alias("is_total_is_skipped"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("play_listener_id").asc_nulls_last(),
    )
)
