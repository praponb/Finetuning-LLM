from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_played_at_to_date_sourc = Window.partitionBy(F.to_date("last_played_at"), "source")

cleaned = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (~F.col("source").isin("search", "album")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

scored = (
    cleaned
    .filter(F.col("is_skipped"))
    .select(
        "*",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.ms_played_bucket",
        "s.source_or_unknown",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.moods").alias("track_moods"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.max_play_ms_played", "p.last_played_at")
)

screened = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.avg("max_play_ms_played").alias("overall_avg_max_play_ms_played")))
    .filter(F.col("max_play_ms_played") > F.col("overall_avg_max_play_ms_played"))
    .drop("overall_avg_max_play_ms_played")
)

metrics = (
    screened
    .select(
        "id",
        "ms_played",
        "source_or_unknown",
        "track_is_explicit",
        "track_moods",
        F.size(F.collect_set("listener_id").over(w_last_played_at_to_date_sourc)).alias("distinct_listener_id_in_last_played_at_to_date_s"),
        F.sum("ms_played").over(w_last_played_at_to_date_sourc).alias("last_played_at_to_date_source_sum_ms_played"),
        F.size(F.collect_set("track_id").over(w_last_played_at_to_date_sourc)).alias("distinct_track_id_in_last_played_at_to_date_sour"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ms_played",
        "source_or_unknown",
        "track_is_explicit",
        "track_moods",
        "last_played_at_to_date_source_sum_ms_played",
        "distinct_track_id_in_last_played_at_to_date_sour",
        F.initcap(F.trim(F.col("source_or_unknown"))).alias("source_or_unknown_clean"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
    .orderBy(F.col("distinct_track_id_in_last_played_at_to_date_sour").desc(), "id")
    .limit(5)
)
