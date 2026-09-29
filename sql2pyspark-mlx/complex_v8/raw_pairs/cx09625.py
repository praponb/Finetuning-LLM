from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_play_device_by_signup_d = Window.partitionBy("plan", "play_device").orderBy(F.col("signup_date").desc(), "id", "play_id").rowsBetween(-3, -1)
w_plan_play_device_by_signup_d_2 = Window.partitionBy("plan", "play_device").orderBy(F.col("signup_date").desc(), "id", "play_id").rowsBetween(-6, -1)
w_play_source_play_device = Window.partitionBy("play_source", "play_device")

valid_listeners = (
    df_listeners
    .filter((F.col("plan").isin("free", "student")) & (F.col("country").isin("UK", "KR")))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        F.substring(F.trim(F.col("country")), 1, 4).alias("country_prefix"),
        F.substring(F.trim(F.col("username")), 1, 2).alias("username_prefix"),
    )
)

with_activity = (
    valid_listeners.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.track_id") >= 21) & (F.col("p.device").isin("car", "speaker")), "left_semi")
)

scored = (
    with_activity
    .filter(F.col("username").isNotNull())
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.quarter(F.col("signup_date")).alias("signup_date_quarter"),
        F.concat_ws(": ", F.col("username"), F.col("username_prefix")).alias("username_username_prefix_label"),
    )
)

detail_rows_plays = (
    scored.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.username",
        "s.plan",
        "s.signup_date",
        "s.interests",
        "s.username_prefix",
        "s.days_since_signup_date",
        "s.signup_date_quarter",
        "s.username_username_prefix_label",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

windowed = (
    detail_rows_plays
    .select(
        "*",
        F.round(F.avg("play_ms_played").over(w_plan_play_device_by_signup_d), 2).alias("play_ms_played_avg_3_prior"),
        F.round(F.avg("signup_date_quarter").over(w_plan_play_device_by_signup_d_2), 2).alias("signup_date_quarter_avg_6_prior"),
        F.round(F.col("signup_date_quarter") / F.nullif(F.sum("signup_date_quarter").over(w_play_source_play_device), F.lit(0)), 4).alias("signup_date_quarter_share_of_play_source_play_de"),
    )
)

exploded_detail_plays = (
    windowed.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.username",
        "s.plan",
        "s.interests",
        "s.username_prefix",
        "s.days_since_signup_date",
        "s.signup_date_quarter",
        "s.username_username_prefix_label",
        "s.play_id",
        "s.play_device",
        "s.play_ms_played",
        "s.play_ms_played_avg_3_prior",
        "s.signup_date_quarter_avg_6_prior",
        "s.signup_date_quarter_share_of_play_source_play_de",
        F.col("p.id").alias("play_id_2"),
        F.col("p.source").alias("play_source_2"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

profiled = (
    exploded_detail_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

result = (
    profiled
    .groupBy("play_device", "play_id_2")
    .pivot("plan", ["family", "free"])
    .agg(F.min("signup_date_quarter"))
    .orderBy("play_device", "play_id_2")
)
