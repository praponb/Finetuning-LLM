from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_by_signup_date = Window.partitionBy("play_id").orderBy("signup_date", "id", "play_id")
w_play_is_skipped = Window.partitionBy("play_is_skipped")

cleaned_listeners = (
    df_listeners
    .filter(F.col("plan").isin("student", "free"))
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
    )
)

enriched = (
    cleaned_listeners
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("plan")), " ", "_")).alias("plan_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

exploded_detail_plays = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (~F.col("p.is_skipped")), "inner")
    .select(
        "s.id",
        "s.signup_date",
        "s.plan_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

with_windows = (
    exploded_detail_plays
    .select(
        "*",
        F.row_number().over(w_play_id_by_signup_date).alias("rn"),
        F.sum("play_ms_played").over(w_play_is_skipped).alias("play_is_skipped_sum_play_ms_played"),
        F.round(F.col("play_ms_played") / F.nullif(F.sum("play_ms_played").over(w_play_is_skipped), F.lit(0)), 4).alias("play_ms_played_share_of_play_is_skipped"),
        F.datediff(F.col("signup_date"), F.lag("signup_date").over(w_play_id_by_signup_date)).alias("days_since_prev_signup_date"),
        F.lead("signup_date", 3).over(w_play_id_by_signup_date).alias("next3_signup_date"),
    )
)

exploded_detail = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.signup_date",
        "s.plan_slug",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_ms_played_share_of_play_is_skipped",
        "s.next3_signup_date",
        F.col("p.id").alias("play_id_2"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played_2"),
    )
)

scored = (
    exploded_detail
    .select(
        "id",
        "signup_date",
        "play_id",
        "play_ms_played_share_of_play_is_skipped",
        "next3_signup_date",
        "play_id_2",
        "play_source",
        "play_device",
        "play_ms_played_2",
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("play_source")), " ", "_")).alias("play_source_slug"),
        F.dayofweek(F.col("signup_date")).alias("signup_date_dayofweek"),
    )
)

result = (
    scored
    .select(
        "id",
        "signup_date",
        "play_ms_played_share_of_play_is_skipped",
        "next3_signup_date",
        "play_id_2",
        "play_source",
        "play_device",
        "play_ms_played_2",
    )
    .orderBy(F.col("play_ms_played_share_of_play_is_skipped").desc())
)
