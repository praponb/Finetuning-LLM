from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_birth_year_tier_country_by_s = Window.partitionBy("birth_year_tier", "country").orderBy(F.col("signup_date_plus_6m").asc_nulls_last(), "id")
w_birth_year_tier_country_by_s_2 = Window.partitionBy("birth_year_tier", "country").orderBy(F.col("signup_date_plus_6m").asc_nulls_last())
w_all_by_signup_date_plus_6m_t = Window.orderBy("signup_date_plus_6m")
w_all_by_signup_date_plus_6m = Window.orderBy("signup_date_plus_6m", "id")
w_all_by_signup_date_plus_6m_f = Window.orderBy("signup_date_plus_6m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_by_signup_date_plus_6m = Window.partitionBy("plan").orderBy("signup_date_plus_6m", "id")
w_signup_date_plus_6m_seq_plan = Window.partitionBy("signup_date_plus_6m_seq", "plan").orderBy(F.col("signup_date_plus_6m_seq").desc_nulls_first())
w_all_by_signup_date_plus_6m_p = Window.orderBy("signup_date_plus_6m_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_plus_6m_seq_plan_2 = Window.partitionBy("signup_date_plus_6m_seq", "plan").orderBy(F.col("signup_date_plus_6m_seq").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)

recent_listeners = (
    df_listeners
    .filter(F.year(F.col("signup_date")) == 2025)
    .select(
        "id",
        "username",
        "plan",
        "country",
        F.when(F.col("birth_year") >= 1980, "HIGH").when(F.col("birth_year") >= 1975, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
        F.add_months(F.col("signup_date"), 6).alias("signup_date_plus_6m"),
    )
)

with_windows = (
    recent_listeners
    .select(
        "id",
        "plan",
        "country",
        "birth_year_tier",
        F.row_number().over(w_birth_year_tier_country_by_s).alias("signup_date_plus_6m_seq"),
        F.datediff(F.col("signup_date_plus_6m"), F.lag("signup_date_plus_6m").over(w_birth_year_tier_country_by_s)).alias("days_since_prev_signup_date_plus_6m"),
        F.dense_rank().over(w_birth_year_tier_country_by_s_2).alias("signup_date_plus_6m_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_signup_date_plus_6m_t), 3).alias("signup_date_plus_6m_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_signup_date_plus_6m), 3).alias("signup_date_plus_6m_percent_rank_2"),
        F.round(F.cume_dist().over(w_birth_year_tier_country_by_s_2), 3).alias("signup_date_plus_6m_cume_dist"),
        F.datediff(F.col("signup_date_plus_6m"), F.lag("signup_date_plus_6m").over(w_all_by_signup_date_plus_6m)).alias("days_since_prev_signup_date_plus_6m_2"),
        F.nth_value("signup_date_plus_6m", 2).over(w_all_by_signup_date_plus_6m_f).alias("second_signup_date_plus_6m"),
        F.lag("signup_date_plus_6m").over(w_plan_by_signup_date_plus_6m).alias("prev_signup_date_plus_6m"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.round(F.percent_rank().over(w_signup_date_plus_6m_seq_plan), 3).alias("signup_date_plus_6m_seq_percent_rank"),
        F.nth_value("prev_signup_date_plus_6m", 2).over(w_all_by_signup_date_plus_6m_p).alias("second_prev_signup_date_plus_6m"),
        F.round(F.avg("signup_date_plus_6m_seq").over(w_signup_date_plus_6m_seq_plan_2), 2).alias("signup_date_plus_6m_seq_avg_4"),
    )
)

result = (
    metrics
    .filter(F.col("days_since_prev_signup_date_plus_6m") >= 80)
    .select(
        "id",
        "country",
        "signup_date_plus_6m_seq",
        "days_since_prev_signup_date_plus_6m",
        "signup_date_plus_6m_dense_rank",
        "signup_date_plus_6m_percent_rank_2",
        "signup_date_plus_6m_cume_dist",
        "days_since_prev_signup_date_plus_6m_2",
        "second_signup_date_plus_6m",
        "prev_signup_date_plus_6m",
        "second_prev_signup_date_plus_6m",
        "signup_date_plus_6m_seq_avg_4",
    )
    .orderBy(
        F.col("second_prev_signup_date_plus_6m").asc_nulls_last(),
        "signup_date_plus_6m_cume_dist",
    )
)
