from pyspark.sql import functions as F

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

screened = (
    base
    .crossJoin(base.agg(F.max("ms_played").alias("overall_max_ms_played")))
    .filter(F.col("ms_played") < F.col("overall_max_ms_played"))
    .drop("overall_max_ms_played")
)

line_level_plays = (
    screened.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") == "car"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.played_at_plus_1d",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    line_level_plays
    .groupBy("play_id", "listener_id")
    .pivot("play_source", ["album", "radio"])
    .agg(F.sum("ms_played"))
    .orderBy("play_id", "listener_id")
)
