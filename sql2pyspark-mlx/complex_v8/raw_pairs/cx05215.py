from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_plan_by_signup_date = Window.partitionBy("country", "plan").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_signup_date_month_country = Window.partitionBy(F.date_trunc("month", "signup_date"), "country")
w_all_by_signup_date_moving_6 = Window.orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w_plan_country_by_signup_date = Window.partitionBy("plan", "country").orderBy("signup_date", "id")
w_country_plan = Window.partitionBy("country", "plan")
w_plan_by_birth_year_running = Window.partitionBy("plan").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_birth_year_prior_2 = Window.orderBy(F.col("birth_year").desc(), "id").rowsBetween(-2, -1)
w_country_plan_by_signup_date_2 = Window.partitionBy("country", "plan").orderBy(F.col("signup_date").desc())

filtered = (
    df_listeners
    .filter((F.col("plan").isin("premium", "family", "free")) & (F.col("birth_year").between(1967, 1994)))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.sum("birth_year").over(w_country_plan_by_signup_date).alias("running_birth_year"),
        F.round(F.col("birth_year") - F.avg("birth_year").over(w_signup_date_month_country), 2).alias("birth_year_vs_signup_date_month_country_avg"),
        F.sum("birth_year").over(w_all_by_signup_date_moving_6).alias("birth_year_sum_7"),
        F.size(F.collect_set("plan").over(w_signup_date_month_country)).alias("distinct_plan_in_signup_date_month_country"),
        F.lag("birth_year", 1, 0).over(w_plan_country_by_signup_date).alias("prev_birth_year"),
        F.count(F.lit(1)).over(w_signup_date_month_country).alias("rows_in_signup_date_month_country"),
        F.round(F.avg("birth_year").over(w_signup_date_month_country), 2).alias("signup_date_month_country_avg_birth_year"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_country_plan), F.lit(0)), 4).alias("birth_year_share_of_country_plan"),
        F.sum("birth_year").over(w_country_plan).alias("country_plan_sum_birth_year"),
        F.dense_rank().over(w_plan_country_by_signup_date).alias("signup_date_dense_rank"),
        F.first("signup_date", ignorenulls=True).over(w_plan_by_birth_year_running).alias("first_known_signup_date"),
        F.round(F.col("birth_year") - F.avg("birth_year").over(w_country_plan), 2).alias("birth_year_vs_country_plan_avg"),
        F.count(F.lit(1)).over(w_country_plan).alias("rows_in_country_plan"),
        F.round(F.avg("birth_year").over(w_all_by_birth_year_prior_2), 2).alias("birth_year_avg_2_prior"),
        F.round(F.percent_rank().over(w_country_plan_by_signup_date_2), 3).alias("signup_date_percent_rank"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("signup_date_dense_rank") <= 3)
    .drop("signup_date_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "username",
        "plan",
        "country",
        "interests",
        "running_birth_year",
        "birth_year_vs_signup_date_month_country_avg",
        "distinct_plan_in_signup_date_month_country",
        "rows_in_signup_date_month_country",
        "signup_date_month_country_avg_birth_year",
        "birth_year_share_of_country_plan",
        "country_plan_sum_birth_year",
        "first_known_signup_date",
        "birth_year_avg_2_prior",
        "signup_date_percent_rank",
        F.date_add(F.col("signup_date"), 30).alias("signup_date_plus_30d"),
        F.date_trunc("quarter", F.col("first_known_signup_date")).alias("first_known_signup_date_quarter"),
    )
    .orderBy(
        F.col("birth_year_avg_2_prior").desc(),
        F.col("birth_year_share_of_country_plan").asc(),
    )
)
