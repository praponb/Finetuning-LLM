from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .filter((F.col("country").isNotNull()) & (F.length(F.trim(F.col("username"))) >= 8))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "interests",
        F.date_format(F.col("signup_date"), "yyyy-MM").alias("signup_date_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

with_children_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.track_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.username",
        "s.signup_date",
        "s.interests",
        "s.signup_date_label",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

totals = (
    with_children_plays
    .groupBy(
        F.date_trunc("month", "signup_date").alias("signup_date_month"),
        "play_track_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("play_ms_played"), 2).alias("avg_play_ms_played"),
        F.sum(F.when(F.col("play_device") == "speaker", F.col("play_ms_played")).otherwise(0)).alias("play_ms_played_play_device_speaker"),
        F.countDistinct("play_track_id").alias("distinct_play_track_ids"),
        F.min("signup_date").alias("first_signup_date"),
        F.sum(F.when(F.length(F.trim(F.col("username"))) >= 7, F.col("play_ms_played"))).alias("play_ms_played_length_trim_username_7"),
        F.max("play_ms_played").alias("max_play_ms_played_check"),
    )
    .filter(F.col("max_play_ms_played_check") > 52009)
    .drop("max_play_ms_played_check")
)

qualified = (
    totals.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.play_track_id")) & (F.col("p.ms_played") >= 72464), "left_semi")
)

screened = (
    qualified
    .crossJoin(qualified.agg(F.avg("avg_play_ms_played").alias("overall_avg_avg_play_ms_played")))
    .filter(F.col("avg_play_ms_played") > F.col("overall_avg_avg_play_ms_played"))
    .drop("overall_avg_avg_play_ms_played")
)

result = (
    screened
    .select(
        "signup_date_month",
        "play_track_id",
        "row_count",
        "avg_play_ms_played",
        "play_ms_played_play_device_speaker",
        "distinct_play_track_ids",
        "first_signup_date",
    )
    .orderBy("signup_date_month", "play_track_id")
    .limit(5)
)
