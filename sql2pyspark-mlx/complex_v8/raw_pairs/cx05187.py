from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_birth_year_full = Window.orderBy(F.col("birth_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_month_by_birth_y = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy(F.col("birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_birth_year_running = Window.orderBy(F.col("birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_birth_year = Window.orderBy(F.col("birth_year").desc(), "id")
w_signup_date_quarter = Window.partitionBy("signup_date_quarter")
w_plan_signup_date_quarter = Window.partitionBy("plan", "signup_date_quarter")
w_plan_country_by_signup_date = Window.partitionBy("plan", "country").orderBy("signup_date_quarter", "id")
w_seq_no_by_birth_year_vs_sign = Window.partitionBy("seq_no").orderBy("birth_year_vs_signup_date_quarter_avg", "id")
w_signup_date_quarter_signup_d = Window.partitionBy("signup_date_quarter", F.year("signup_date"))
w_last_known_signup_date_quart = Window.partitionBy("last_known_signup_date_quarter", "signup_date_quarter")
w_all_by_signup_date_quarter_m = Window.orderBy(F.col("signup_date_quarter_max_signup_date_quarter").desc(), "id")

recent_listeners = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 2)
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        F.quarter(F.col("signup_date")).alias("signup_date_quarter"),
    )
)

windowed = (
    recent_listeners
    .select(
        "*",
        F.nth_value("signup_date", 2).over(w_all_by_birth_year_full).alias("second_signup_date"),
        F.sum("signup_date_quarter").over(w_signup_date_month_by_birth_y).alias("running_signup_date_quarter"),
        F.last("signup_date_quarter", ignorenulls=True).over(w_all_by_birth_year_running).alias("last_known_signup_date_quarter"),
        F.row_number().over(w_all_by_birth_year).alias("seq_no"),
        F.count(F.lit(1)).over(w_signup_date_quarter).alias("rows_in_signup_date_quarter"),
        F.max("signup_date_quarter").over(w_signup_date_quarter).alias("signup_date_quarter_max_signup_date_quarter"),
        F.min("birth_year").over(w_plan_signup_date_quarter).alias("plan_signup_date_quarter_min_birth_year"),
        F.round(F.col("birth_year") - F.avg("birth_year").over(w_signup_date_quarter), 2).alias("birth_year_vs_signup_date_quarter_avg"),
        F.count(F.lit(1)).over(w_all_by_birth_year_running).alias("running_count"),
        F.row_number().over(w_plan_country_by_signup_date).alias("rn"),
    )
)

first_rows = windowed.filter(F.col("rn") == 1)

ranked = (
    first_rows
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "running_signup_date_quarter",
        "last_known_signup_date_quarter",
        "seq_no",
        "rows_in_signup_date_quarter",
        "signup_date_quarter_max_signup_date_quarter",
        "plan_signup_date_quarter_min_birth_year",
        "rn",
        (F.col("birth_year") - F.lag("birth_year").over(w_seq_no_by_birth_year_vs_sign)).alias("birth_year_change"),
        F.count(F.lit(1)).over(w_signup_date_quarter_signup_d).alias("rows_in_signup_date_quarter_signup_date_year"),
        F.size(F.collect_set("plan").over(w_last_known_signup_date_quart)).alias("distinct_plan_in_last_known_signup_date_quarter_"),
        F.round(F.percent_rank().over(w_all_by_signup_date_quarter_m), 3).alias("signup_date_quarter_max_signup_date_quarter_perc"),
    )
)

result = (
    ranked
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "running_signup_date_quarter",
        "last_known_signup_date_quarter",
        "seq_no",
        "rows_in_signup_date_quarter",
        "signup_date_quarter_max_signup_date_quarter",
        "plan_signup_date_quarter_min_birth_year",
        "rn",
        "rows_in_signup_date_quarter_signup_date_year",
        "distinct_plan_in_last_known_signup_date_quarter_",
        F.coalesce(F.col("birth_year_change"), F.lit(0)).alias("birth_year_change_filled"),
    )
    .orderBy(F.col("seq_no").asc_nulls_last(), "birth_year_change_filled")
)
