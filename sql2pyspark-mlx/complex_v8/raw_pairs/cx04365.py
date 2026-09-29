from pyspark.sql import functions as F

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.month(F.col("played_at")).alias("played_at_month"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "played_at_month",
        "played_at_label",
        "device_slug",
        F.greatest(F.col("ms_played"), F.col("played_at_month")).alias("greatest_ms_played_played_at_month"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

summarized_rows = (
    annotated.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

exploded_detail = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.played_at_month",
        "s.played_at_label",
        "s.device_slug",
        "s.greatest_ms_played_played_at_month",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["source", "listener_id", "played_at_month"], ["source"], ["listener_id"]],
        "source", "listener_id", "played_at_month",
    )
    .agg(
        F.sum(F.when(F.col("source") == "playlist", F.col("ms_played")).otherwise(0)).alias("ms_played_source_playlist"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("played_at_month"), F.lit(0)), 3).alias("ms_played_to_played_at_month_ratio"),
    )
    .orderBy(
        F.col("source").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
        F.col("played_at_month").asc_nulls_last(),
    )
)
