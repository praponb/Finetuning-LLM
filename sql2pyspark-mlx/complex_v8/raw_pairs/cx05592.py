from pyspark.sql import functions as F

scoped_listeners = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.concat(F.col("plan"), F.lit("#"), F.col("birth_year").cast("string")).alias("plan_ref"),
    )
)

detail_rows_plays = (
    scoped_listeners.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.plan",
        "s.birth_year",
        "s.interests",
        "s.plan_ref",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

combined_details_play = (
    detail_rows_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("p.listener_id"), "left")
    .select(
        "s.id",
        "s.plan",
        "s.interests",
        "s.plan_ref",
        "s.play_id",
        "s.play_device",
        F.col("p.played_at").alias("play_played_at"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

with_details_play = (
    combined_details_play.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.*",
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_details_play
    .groupBy("play_id")
    .pivot("plan", ["family", "premium", "free"])
    .agg(F.min("play_ms_played").alias("min_v"), F.count("play_ms_played").alias("n"))
    .orderBy("play_id")
)
