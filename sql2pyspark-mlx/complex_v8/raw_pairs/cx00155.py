from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_signup_date_month_by = Window.partitionBy("country", F.date_trunc("month", "signup_date")).orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_month_by_signup = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy(F.col("signup_date").asc_nulls_last(), "id")
w_country_by_signup_date_full = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_month_by_signup_2 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_signup_date_month_by_2 = Window.partitionBy("country", F.date_trunc("month", "signup_date")).orderBy("signup_date", "id")
w_signup_date_month_by_signup_3 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("signup_date")
w_signup_date_month_by_signup_4 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("signup_date", "id")
w_signup_date_month_by_signup_5 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_signup_date_month = Window.partitionBy(F.month("second_signup_date"))
w_signup_date_tercile_signup_d = Window.partitionBy("signup_date_tercile", "signup_date_rank").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_listeners
    .filter((F.col("plan").isin("premium", "free", "family")) & (F.col("birth_year") <= 1988))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "interests",
    )
)

metrics = (
    base
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "interests",
        F.first("signup_date").over(w_country_signup_date_month_by).alias("first_signup_date"),
        F.lead("signup_date").over(w_signup_date_month_by_signup).alias("next_signup_date"),
        F.nth_value("signup_date", 2).over(w_country_by_signup_date_full).alias("second_signup_date"),
        F.first("signup_date", ignorenulls=True).over(w_signup_date_month_by_signup_2).alias("first_known_signup_date"),
        F.rank().over(w_country_signup_date_month_by_2).alias("signup_date_rank"),
        F.ntile(3).over(w_country_signup_date_month_by_2).alias("signup_date_tercile"),
        F.round(F.percent_rank().over(w_signup_date_month_by_signup_3), 3).alias("signup_date_percent_rank"),
        F.datediff(F.col("signup_date"), F.lag("signup_date").over(w_signup_date_month_by_signup_4)).alias("days_since_prev_signup_date"),
        F.nth_value("signup_date", 2).over(w_signup_date_month_by_signup_5).alias("second_signup_date_2"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "username",
        "country",
        "interests",
        "first_signup_date",
        "next_signup_date",
        "first_known_signup_date",
        "signup_date_rank",
        "signup_date_tercile",
        "signup_date_percent_rank",
        "days_since_prev_signup_date",
        "second_signup_date_2",
        F.count(F.lit(1)).over(w_second_signup_date_month).alias("rows_in_second_signup_date_month"),
        F.size(F.collect_set("country").over(w_second_signup_date_month)).alias("distinct_country_in_second_signup_date_month"),
        F.min("days_since_prev_signup_date").over(w_second_signup_date_month).alias("second_signup_date_month_min_days_since_prev_sig"),
        F.min("signup_date_rank").over(w_signup_date_tercile_signup_d).alias("min_signup_date_rank_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "username",
        "country",
        "interests",
        "first_signup_date",
        "next_signup_date",
        "first_known_signup_date",
        "signup_date_rank",
        "signup_date_tercile",
        "signup_date_percent_rank",
        "days_since_prev_signup_date",
        "second_signup_date_2",
        "distinct_country_in_second_signup_date_month",
        "second_signup_date_month_min_days_since_prev_sig",
        "min_signup_date_rank_so_far",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
    .orderBy(
        F.col("days_since_prev_signup_date").desc(),
        F.col("signup_date_rank").desc(),
    )
)
