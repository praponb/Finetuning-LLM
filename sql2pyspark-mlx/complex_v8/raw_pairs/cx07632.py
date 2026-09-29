from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_by_signup_date = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w_country_by_signup_date_2 = Window.partitionBy("country").orderBy(F.col("signup_date").asc_nulls_last(), "id")
w_country_by_signup_date_full = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_rank_signup_date = Window.partitionBy("signup_date_rank", "signup_date_quartile").orderBy("signup_date_quartile")
w_second_signup_date_month_by = Window.partitionBy(F.month("second_signup_date")).orderBy(F.col("signup_date").desc())
w_signup_date_rank_signup_date_2 = Window.partitionBy("signup_date_rank", "signup_date_quartile").orderBy("signup_date_quartile", "id")
w_second_signup_date_month = Window.partitionBy(F.month("second_signup_date"))

base_listeners = (
    df_listeners
    .filter((F.col("birth_year") >= 1969) & (F.col("plan") != "family"))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "interests",
    )
)

windowed = (
    base_listeners
    .select(
        "id",
        "country",
        "signup_date",
        "interests",
        F.row_number().over(w_country_by_signup_date).alias("signup_date_seq"),
        F.rank().over(w_country_by_signup_date).alias("signup_date_rank"),
        F.round(F.percent_rank().over(w_country_by_signup_date), 3).alias("signup_date_percent_rank"),
        F.ntile(4).over(w_country_by_signup_date_2).alias("signup_date_quartile"),
        F.nth_value("signup_date", 2).over(w_country_by_signup_date_full).alias("second_signup_date"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_signup_date_rank_signup_date), 3).alias("signup_date_quartile_cume_dist"),
        F.round(F.percent_rank().over(w_second_signup_date_month_by), 3).alias("signup_date_percent_rank_2"),
        F.lead("signup_date").over(w_signup_date_rank_signup_date_2).alias("next_signup_date"),
        F.count(F.lit(1)).over(w_second_signup_date_month).alias("rows_in_second_signup_date_month"),
        F.lag("signup_date_quartile").over(w_signup_date_rank_signup_date_2).alias("prev_signup_date_quartile"),
    )
)

country_stats = (
    with_windows
    .groupBy("country")
    .agg(F.max("prev_signup_date_quartile").alias("max_prev_signup_date_quartile"))
)

vs_group = (
    with_windows.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.prev_signup_date_quartile") == F.col("g.max_prev_signup_date_quartile"))
    .select(
        "s.*",
        "g.max_prev_signup_date_quartile",
        F.round(F.col("s.prev_signup_date_quartile") - F.col("g.max_prev_signup_date_quartile"), 2).alias("prev_signup_date_quartile_minus_max"),
    )
)

detail_rows_plays = (
    vs_group.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.ms_played") >= 120663), "inner")
    .select(
        "s.id",
        "s.signup_date",
        "s.interests",
        "s.signup_date_seq",
        "s.signup_date_rank",
        "s.signup_date_percent_rank",
        "s.second_signup_date",
        "s.signup_date_percent_rank_2",
        "s.next_signup_date",
        "s.prev_signup_date_quartile",
        "s.prev_signup_date_quartile_minus_max",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

enriched = (
    detail_rows_plays
    .filter(F.col("prev_signup_date_quartile_minus_max") >= -6.0)
    .select(
        "*",
        F.year(F.col("next_signup_date")).alias("next_signup_date_year"),
        F.abs(F.col("play_ms_played") - 214343).alias("play_ms_played_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next_signup_date")).alias("days_since_next_signup_date"),
    )
)

result = (
    enriched
    .select(
        "id",
        "interests",
        "signup_date_seq",
        "signup_date_rank",
        "signup_date_percent_rank",
        "signup_date_percent_rank_2",
        "next_signup_date",
        "prev_signup_date_quartile",
        "prev_signup_date_quartile_minus_max",
        "play_id",
        "play_ms_played",
        "play_ms_played_distance",
        "days_since_next_signup_date",
        F.coalesce(F.col("signup_date_percent_rank"), F.lit(0.0)).alias("signup_date_percent_rank_filled"),
        F.coalesce(F.col("signup_date_rank"), F.lit(0)).alias("signup_date_rank_filled"),
    )
    .orderBy(
        F.col("signup_date_rank_filled").desc(),
        F.col("signup_date_percent_rank_filled").desc(),
    )
)
