from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date = Window.orderBy(F.col("signup_date").desc(), "id")
w_all_by_signup_date_2 = Window.orderBy("signup_date", "id")
w_plan_country = Window.partitionBy("plan", "country")
w_plan_by_signup_date = Window.partitionBy("plan").orderBy("signup_date", "id")
w_all_by_signup_date_running = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_plan_by_signup_date_running = Window.partitionBy("plan").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_plan_by_signup_date_full = Window.partitionBy("plan").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_signup_date_full = Window.orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_rank = Window.partitionBy("signup_date_rank")
w_first_known_signup_date_mont = Window.partitionBy(F.date_trunc("month", "first_known_signup_date")).orderBy("prev2_signup_date")
w_all_by_signup_date_ties = Window.orderBy(F.col("signup_date").desc())
w_first_known_signup_date_mont_2 = Window.partitionBy(F.date_trunc("month", "first_known_signup_date")).orderBy("prev2_signup_date", "id").rowsBetween(-6, -1)

base = (
    df_listeners
    .filter((~F.col("plan").isin("student", "free")) | (F.col("signup_date") >= F.lit("2024-09-19").cast("date")))
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

metrics = (
    base
    .select(
        "*",
        F.lag("signup_date", 2).over(w_all_by_signup_date).alias("prev2_signup_date"),
        F.round(F.cume_dist().over(w_all_by_signup_date_2), 3).alias("signup_date_cume_dist"),
        F.count(F.lit(1)).over(w_plan_country).alias("rows_in_plan_country"),
        F.row_number().over(w_plan_by_signup_date).alias("rn"),
        F.count(F.lit(1)).over(w_all_by_signup_date_running).alias("running_count"),
        F.max("birth_year").over(w_plan_country).alias("plan_country_max_birth_year"),
        F.first("signup_date", ignorenulls=True).over(w_plan_by_signup_date_running).alias("first_known_signup_date"),
        F.nth_value("birth_year", 2).over(w_plan_by_signup_date_full).alias("second_birth_year"),
        F.rank().over(w_all_by_signup_date_2).alias("signup_date_rank"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.last("second_birth_year").over(w_all_by_signup_date_full).alias("last_second_birth_year"),
        F.count(F.lit(1)).over(w_signup_date_rank).alias("rows_in_signup_date_rank"),
        F.rank().over(w_first_known_signup_date_mont).alias("prev2_signup_date_rank"),
        F.round(F.cume_dist().over(w_all_by_signup_date_ties), 3).alias("signup_date_cume_dist_2"),
        F.round(F.avg("running_count").over(w_first_known_signup_date_mont_2), 2).alias("running_count_avg_6_prior"),
        F.datediff(F.col("signup_date"), F.lag("signup_date").over(w_all_by_signup_date)).alias("days_since_prev_signup_date"),
        F.min("running_count").over(w_signup_date_rank).alias("signup_date_rank_min_running_count"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        "prev2_signup_date",
        "signup_date_cume_dist",
        "rows_in_plan_country",
        "rn",
        "first_known_signup_date",
        "second_birth_year",
        "signup_date_rank",
        "last_second_birth_year",
        "rows_in_signup_date_rank",
        "signup_date_cume_dist_2",
        "running_count_avg_6_prior",
        "days_since_prev_signup_date",
        "signup_date_rank_min_running_count",
        F.array_contains(F.col("interests"), "kpop").alias("has_kpop"),
    )
    .orderBy("plan")
)
