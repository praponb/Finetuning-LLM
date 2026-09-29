from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date_ties = Window.orderBy("signup_date")
w_signup_date_year_signup_date = Window.partitionBy(F.year("signup_date"), "signup_date_year").orderBy("birth_year", "id")
w_all_by_birth_year_running = Window.orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_birth_year = Window.orderBy(F.col("birth_year").desc(), "id")
w_signup_date_year_signup_date_2 = Window.partitionBy(F.year("signup_date"), "signup_date_year").orderBy("birth_year")
w_all_by_birth_year_full = Window.orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_country = Window.partitionBy("plan", "country")
w_all_by_signup_date_full = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_birth_year_ties = Window.orderBy("birth_year")
w_signup_date_year_signup_date_3 = Window.partitionBy(F.year("signup_date"), "signup_date_year").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_birth_year_rank = Window.partitionBy("plan", "birth_year_rank")
w_last_signup_date_month_by_se = Window.partitionBy(F.date_trunc("month", "last_signup_date")).orderBy(F.col("seq_no").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_signup_date_year_by_last_sig = Window.partitionBy("signup_date_year").orderBy(F.col("last_signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_listeners
    .filter(F.col("username").isNotNull())
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.year(F.col("signup_date")).alias("signup_date_year"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_signup_date_ties), 3).alias("signup_date_cume_dist"),
        F.row_number().over(w_signup_date_year_signup_date).alias("seq_no"),
        F.first("signup_date", ignorenulls=True).over(w_all_by_birth_year_running).alias("first_known_signup_date"),
        F.dense_rank().over(w_all_by_birth_year).alias("birth_year_dense_rank"),
        F.rank().over(w_signup_date_year_signup_date_2).alias("birth_year_rank"),
        F.last("birth_year").over(w_all_by_birth_year_full).alias("last_birth_year"),
        F.min("birth_year").over(w_plan_country).alias("plan_country_min_birth_year"),
        F.first("birth_year").over(w_all_by_signup_date_full).alias("first_birth_year"),
        F.round(F.percent_rank().over(w_all_by_birth_year_ties), 3).alias("birth_year_percent_rank"),
        F.last("signup_date").over(w_signup_date_year_signup_date_3).alias("last_signup_date"),
    )
)

first_rows = with_trends.filter(F.col("seq_no") <= 5)

metrics = (
    first_rows
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "signup_date_cume_dist",
        "seq_no",
        "birth_year_dense_rank",
        "first_birth_year",
        "birth_year_percent_rank",
        "last_signup_date",
        F.size(F.collect_set("country").over(w_plan_birth_year_rank)).alias("distinct_country_in_plan_birth_year_rank"),
        F.count(F.lit(1)).over(w_plan_birth_year_rank).alias("rows_in_plan_birth_year_rank"),
        F.last("first_birth_year", ignorenulls=True).over(w_last_signup_date_month_by_se).alias("last_known_first_birth_year"),
        F.nth_value("birth_year_percent_rank", 2).over(w_signup_date_year_by_last_sig).alias("second_birth_year_percent_rank"),
        F.round(F.col("signup_date_cume_dist") - F.avg("signup_date_cume_dist").over(w_plan_birth_year_rank), 2).alias("signup_date_cume_dist_vs_plan_birth_year_rank_av"),
        F.round(F.avg("birth_year_dense_rank").over(w_plan_birth_year_rank), 2).alias("plan_birth_year_rank_avg_birth_year_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "country",
        "signup_date",
        "signup_date_cume_dist",
        "seq_no",
        "birth_year_dense_rank",
        "distinct_country_in_plan_birth_year_rank",
        "last_known_first_birth_year",
        "second_birth_year_percent_rank",
        "signup_date_cume_dist_vs_plan_birth_year_rank_av",
        "plan_birth_year_rank_avg_birth_year_dense_rank",
        F.concat(F.col("country"), F.lit("#"), F.col("birth_year").cast("string")).alias("country_ref"),
        F.abs(F.col("birth_year") - 1977).alias("birth_year_distance"),
    )
    .orderBy(
        F.col("plan_birth_year_rank_avg_birth_year_dense_rank").asc_nulls_last(),
        F.col("signup_date_cume_dist_vs_plan_birth_year_rank_av").desc(),
    )
)
