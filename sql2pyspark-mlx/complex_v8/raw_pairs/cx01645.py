from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

enriched = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source",
        "s.ms_played_bucket",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

track_id_stats = (
    enriched
    .groupBy("track_id")
    .agg(F.max("ms_played_bucket").alias("max_ms_played_bucket"))
)

compared = (
    enriched.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played_bucket") == F.col("g.max_ms_played_bucket"))
    .select(
        "s.*",
        "g.max_ms_played_bucket",
        F.round(F.col("s.ms_played_bucket") - F.col("g.max_ms_played_bucket"), 2).alias("ms_played_bucket_minus_max"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("E%")), "left_semi")
)

totals = (
    without_matches
    .groupBy(
        F.when(F.col("max_ms_played_bucket") >= 321000.0, "large").when(F.col("max_ms_played_bucket") >= 143250.0, "medium").otherwise("small").alias("max_ms_played_bucket_tier"),
    )
    .agg(
        F.round(F.avg("ms_played_bucket_minus_max"), 2).alias("avg_ms_played_bucket_minus_max"),
        F.round(F.avg("max_ms_played_bucket"), 2).alias("avg_max_ms_played_bucket"),
        F.sum(F.when(F.col("source").isin("radio", "playlist", "search"), 1).otherwise(0)).alias("n_source_in_radio_playlist_sea"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.sum("max_ms_played_bucket").alias("total_max_ms_played_bucket"),
    )
)

screened = (
    totals
    .crossJoin(totals.agg(F.avg("avg_ms_played_bucket_minus_max").alias("overall_avg_avg_ms_played_bucket_minus_max")))
    .filter(F.col("avg_ms_played_bucket_minus_max") > F.col("overall_avg_avg_ms_played_bucket_minus_max"))
    .drop("overall_avg_avg_ms_played_bucket_minus_max")
)

result = (
    screened
    .filter(F.col("max_ms_played_bucket_tier").isNotNull())
    .select(
        "total_max_ms_played_bucket",
        "n_source_in_radio_playlist_sea",
        "source_list",
    )
    .distinct()
    .orderBy("total_max_ms_played_bucket")
)
