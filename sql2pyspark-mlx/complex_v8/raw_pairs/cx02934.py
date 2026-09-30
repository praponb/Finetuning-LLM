from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .filter((F.length(F.trim(F.col("username"))) >= 5) & (F.array_contains(F.col("interests"), "indie")))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.plan",
        "s.signup_date",
        "s.birth_year",
        "s.interests",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

combined_details = (
    detail_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("p.track_id"), "left")
    .select(
        "s.id",
        "s.plan",
        "s.birth_year",
        "s.interests",
        "s.play_id",
        "s.play_track_id",
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    combined_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.play_track_id"), "left")
    .select(
        "s.*",
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "play_id",
        F.col("track_bpm").alias("m_track_bpm"),
        F.col("birth_year").alias("birth_year_v"),
        F.col("play_count").alias("m_play_count"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["m_track_bpm", "birth_year_v", "m_play_count"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "play_id", "metric_name")
)
