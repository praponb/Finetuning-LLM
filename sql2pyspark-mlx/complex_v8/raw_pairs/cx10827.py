from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")
w_country_signup_date_year_by = Window.partitionBy("country", F.year("signup_date")).orderBy(F.col("signup_date").desc(), "id")
w_signup_date_rank = Window.partitionBy("signup_date_rank")
w_country_signup_date_rank_by = Window.partitionBy("country", "signup_date_rank").orderBy("birth_year_distance_vs_country_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_signup_date_rank_by_2 = Window.partitionBy("country", "signup_date_rank").orderBy("birth_year_distance_vs_country_avg", "id").rowsBetween(-2, -1)
w_signup_date_rank_by_birth_ye = Window.partitionBy("signup_date_rank").orderBy("birth_year_distance_vs_country_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_id = Window.partitionBy("play_id")
w_play_id_play_device_by_signu = Window.partitionBy("play_id", "play_device").orderBy(F.col("signup_date_rank_sum_signup_date_rank").asc_nulls_last(), "id", "play_id").rowsBetween(-2, -1)
w_play_id_play_device_by_signu_2 = Window.partitionBy("play_id", "play_device").orderBy(F.col("signup_date_rank_sum_signup_date_rank").asc_nulls_last(), "id", "play_id")
w_play_id_play_device_by_signu_3 = Window.partitionBy("play_id", "play_device").orderBy(F.col("signup_date_rank_sum_signup_date_rank").asc_nulls_last(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_listeners = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "live"))
    .select(
        "id",
        "country",
        "signup_date",
        "interests",
        F.substring(F.trim(F.col("country")), 1, 2).alias("country_prefix"),
        F.abs(F.col("birth_year") - 1980).alias("birth_year_distance"),
    )
)

windowed = (
    recent_listeners
    .select(
        "id",
        "country",
        "interests",
        F.round(F.col("birth_year_distance") - F.avg("birth_year_distance").over(w_country), 2).alias("birth_year_distance_vs_country_avg"),
        F.min("birth_year_distance").over(w_country).alias("country_min_birth_year_distance"),
        F.rank().over(w_country_signup_date_year_by).alias("signup_date_rank"),
        F.round(F.col("birth_year_distance") / F.nullif(F.sum("birth_year_distance").over(w_country), F.lit(0)), 4).alias("birth_year_distance_share_of_country"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "country",
        "interests",
        "birth_year_distance_vs_country_avg",
        "country_min_birth_year_distance",
        F.sum("signup_date_rank").over(w_signup_date_rank).alias("signup_date_rank_sum_signup_date_rank"),
        F.nth_value("signup_date_rank", 2).over(w_country_signup_date_rank_by).alias("second_signup_date_rank"),
        F.size(F.collect_set("country").over(w_signup_date_rank)).alias("distinct_country_in_signup_date_rank"),
        F.round(F.col("signup_date_rank") - F.avg("signup_date_rank").over(w_signup_date_rank), 2).alias("signup_date_rank_vs_signup_date_rank_avg"),
        F.sum("signup_date_rank").over(w_country_signup_date_rank_by_2).alias("signup_date_rank_sum_2_prior"),
        F.last("signup_date_rank", ignorenulls=True).over(w_signup_date_rank_by_birth_ye).alias("last_known_signup_date_rank"),
    )
)

detail_rows = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.interests",
        "s.birth_year_distance_vs_country_avg",
        "s.signup_date_rank_sum_signup_date_rank",
        "s.signup_date_rank_vs_signup_date_rank_avg",
        "s.last_known_signup_date_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
    )
)

ranked = (
    detail_rows
    .select(
        "*",
        F.size(F.collect_set("play_device").over(w_play_id)).alias("distinct_play_device_in_play_id"),
        F.max("last_known_signup_date_rank").over(w_play_id_play_device_by_signu).alias("last_known_signup_date_rank_max_2_prior"),
        F.lag("last_known_signup_date_rank").over(w_play_id_play_device_by_signu_2).alias("prev_last_known_signup_date_rank"),
        F.first("last_known_signup_date_rank").over(w_play_id_play_device_by_signu_3).alias("first_last_known_signup_date_rank"),
        F.size(F.collect_set("play_source").over(w_play_id)).alias("distinct_play_source_in_play_id"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.device") == "car") & (F.col("p.is_skipped")), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "interests",
        "birth_year_distance_vs_country_avg",
        "signup_date_rank_sum_signup_date_rank",
        "signup_date_rank_vs_signup_date_rank_avg",
        "last_known_signup_date_rank",
        "play_is_skipped",
        "play_device",
        "last_known_signup_date_rank_max_2_prior",
        "prev_last_known_signup_date_rank",
        "distinct_play_source_in_play_id",
        F.initcap(F.trim(F.col("play_source"))).alias("play_source_clean"),
    )
    .orderBy(
        F.col("birth_year_distance_vs_country_avg").desc(),
        F.col("prev_last_known_signup_date_rank").desc(),
    )
)
