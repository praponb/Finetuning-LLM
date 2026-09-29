from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_played_at = Window.partitionBy("device").orderBy(F.col("played_at").desc_nulls_first(), "id", "play_id")
w_is_skipped = Window.partitionBy("is_skipped")

recent_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.abs(F.col("ms_played") - 107912).alias("ms_played_distance"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

exploded_detail_plays = (
    recent_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance",
        "s.played_at_label",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

ranked = (
    exploded_detail_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "ms_played_distance",
        "play_id",
        "play_device",
        "play_ms_played",
        "play_is_skipped",
        F.lag("play_ms_played").over(w_device_by_played_at).alias("prev_play_ms_played"),
        F.round(F.col("ms_played_distance") - F.avg("ms_played_distance").over(w_is_skipped), 2).alias("ms_played_distance_vs_is_skipped_avg"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.size(F.collect_set("track_id").over(w_is_skipped)).alias("distinct_track_id_in_is_skipped"),
    )
)

enriched = (
    ranked.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    enriched
    .select(
        "id",
        "play_id",
        "rows_in_is_skipped",
        "listener_birth_year",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["rows_in_is_skipped", "listener_birth_year"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "play_id", "metric")
)
