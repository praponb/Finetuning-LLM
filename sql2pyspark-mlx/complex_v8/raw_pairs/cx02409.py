from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_median_is_skipped_fla = Window.orderBy("median_is_skipped_flag")
w_all_by_median_is_skipped_fla_2 = Window.orderBy("median_is_skipped_flag", "listener_signup_date_month", "is_skipped_flag")
w_last_listener_signup_date_mo = Window.partitionBy(F.month("last_listener_signup_date"), "max_is_skipped_flag").orderBy(F.col("n_rows").desc(), "listener_signup_date_month", "is_skipped_flag")

scoped_plays = (
    df_plays
    .filter(F.col("ms_played") >= 73625)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

enriched = (
    scoped_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.country") == "KR")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_flag",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
    )
)

stats = (
    enriched
    .groupBy(
        F.date_trunc("month", "listener_signup_date").alias("listener_signup_date_month"),
        "is_skipped_flag",
    )
    .agg(
        F.max("listener_signup_date").alias("last_listener_signup_date"),
        F.countDistinct("listener_country").alias("distinct_listener_countrys"),
        F.max("is_skipped_flag").alias("max_is_skipped_flag"),
        F.percentile_approx("is_skipped_flag", 0.5).alias("median_is_skipped_flag"),
        F.round(100 * F.count(F.when(F.col("listener_country") == "KR", 1)) / F.count("*"), 1).alias("pct_listener_country_kr"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

metrics = (
    stats
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_median_is_skipped_fla), 3).alias("median_is_skipped_flag_percent_rank"),
        F.lead("is_skipped_flag", 1, 0).over(w_all_by_median_is_skipped_fla_2).alias("next_is_skipped_flag"),
    )
)

windowed = (
    metrics
    .select(
        "listener_signup_date_month",
        "is_skipped_flag",
        "distinct_listener_countrys",
        "max_is_skipped_flag",
        "median_is_skipped_flag",
        "pct_listener_country_kr",
        "n_rows",
        "median_is_skipped_flag_percent_rank",
        F.ntile(4).over(w_last_listener_signup_date_mo).alias("n_rows_quartile"),
        F.ntile(3).over(w_last_listener_signup_date_mo).alias("n_rows_tercile"),
        F.lead("is_skipped_flag").over(w_last_listener_signup_date_mo).alias("next_is_skipped_flag_2"),
    )
)

result = (
    windowed
    .select(
        "listener_signup_date_month",
        "distinct_listener_countrys",
        "pct_listener_country_kr",
        "n_rows",
        "median_is_skipped_flag_percent_rank",
        "n_rows_quartile",
        "n_rows_tercile",
        "next_is_skipped_flag_2",
    )
    .orderBy("listener_signup_date_month", "distinct_listener_countrys")
)
