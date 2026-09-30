from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_username_len = Window.orderBy(F.col("username_len").asc_nulls_last(), "id")
w_all_by_username_len_full = Window.orderBy(F.col("username_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country = Window.partitionBy("country")

valid_listeners = (
    df_listeners
    .filter((F.col("country").isNotNull()) & (F.col("signup_date").between(F.lit("2024-08-02").cast("date"), F.lit("2025-10-18").cast("date"))))
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        "interests",
        F.when(F.col("country").isin("KR", "BR"), "group_a").when(F.col("country") == "JP", "group_b").otherwise("other").alias("country_group"),
        F.length(F.trim(F.col("username"))).alias("username_len"),
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
    )
)

with_windows = (
    valid_listeners
    .select(
        "*",
        F.lead("birth_year").over(w_all_by_username_len).alias("next_birth_year"),
        F.nth_value("username_len", 2).over(w_all_by_username_len_full).alias("second_username_len"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        F.lag("birth_year").over(w_all_by_username_len).alias("prev_birth_year"),
        F.round(F.avg("birth_year").over(w_country), 2).alias("country_avg_birth_year"),
    )
)

labeled = (
    with_windows
    .filter(F.col("country_avg_birth_year") < 1993.6)
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        "interests",
        "country_group",
        "next_birth_year",
        "second_username_len",
        "rows_in_country",
        "country_avg_birth_year",
        F.coalesce(F.col("rows_in_country"), F.lit(0)).alias("rows_in_country_filled"),
    )
)

line_level_plays = (
    labeled.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.plan",
        "s.birth_year",
        "s.interests",
        "s.country_avg_birth_year",
        "s.rows_in_country_filled",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

result = (
    line_level_plays
    .select(
        "id",
        "play_id",
        F.col("play_ms_played").alias("play_ms_played_v"),
        F.col("birth_year").alias("m_birth_year"),
        F.col("rows_in_country_filled").alias("m_rows_in_country_filled"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_ms_played_v", "m_birth_year", "m_rows_in_country_filled"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "play_id", "metric_name")
)
