from pyspark.sql import functions as F

base = (
    df_plays
    .filter((F.col("source").isNotNull()) & (F.col("ms_played") > 120663))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
        F.abs(F.col("ms_played") - 180879).alias("ms_played_distance"),
    )
)

screened = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
    .filter(~F.col("is_skipped"))
)

detail_rows = (
    screened.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") > 42041), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

grouped = (
    detail_rows
    .groupBy(
        "play_id",
        "play_source",
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
    )
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.round(F.stddev("ms_played_distance"), 2).alias("stddev_ms_played_distance"),
        F.round(100 * F.count(F.when(F.col("source") == "radio", 1)) / F.count("*"), 1).alias("pct_source_radio"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(100 * F.count(F.when(~F.col("play_source").isin("album", "playlist"), 1)) / F.count("*"), 1).alias("pct_play_source_not_in_album_pla"),
    )
)

result = (
    grouped
    .groupBy("play_id")
    .pivot("play_source", ["radio", "album"])
    .agg(F.sum("stddev_ms_played_distance"))
)
