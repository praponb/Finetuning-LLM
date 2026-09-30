from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((~F.col("device").isin("phone", "speaker")) | (F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.year(F.col("played_at")).alias("played_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

exploded_detail = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") >= 49807), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device_len",
        "s.played_at_year",
        "s.source_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    exploded_detail
    .groupBy("listener_id")
    .pivot("play_source", ["search", "playlist", "radio", "album"])
    .agg(F.sum("device_len"))
    .orderBy("listener_id")
)
