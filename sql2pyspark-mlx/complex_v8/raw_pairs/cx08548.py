from pyspark.sql import functions as F

cleaned_listeners = (
    df_listeners
    .filter((F.upper(F.trim(F.col("username"))).like("F%")) & (~F.col("plan").isin("family", "student")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_trunc("month", F.col("signup_date")).alias("signup_date_month"),
    )
)

exploded_detail = (
    cleaned_listeners.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.device") == "desktop"), "left")
    .select(
        "s.id",
        "s.username",
        "s.country",
        "s.signup_date",
        "s.signup_date_month",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

enriched_play = (
    exploded_detail.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.username",
        "s.country",
        "s.signup_date",
        "s.play_id",
        "s.play_played_at",
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.source").alias("play_source"),
    )
)

exploded_detail_plays = (
    enriched_play.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.country",
        "s.signup_date",
        "s.play_id",
        "s.play_track_id",
        "s.play_source",
        F.col("p.id").alias("play_id_2"),
        F.col("p.source").alias("play_source_2"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    exploded_detail_plays
    .rollup("play_id", "play_is_skipped")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("play_id_2").alias("distinct_play_id_2s"),
    )
)
