from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date_plus_12m = Window.orderBy("signup_date_plus_12m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_month = Window.partitionBy(F.month("signup_date"))
w_all_by_signup_date_plus_12m_2 = Window.orderBy("signup_date_plus_12m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_signup_date_plus_12m_3 = Window.orderBy("signup_date_plus_12m", "id")
w_signup_date_year_country_by = Window.partitionBy(F.year("signup_date"), "country").orderBy(F.col("signup_date_plus_12m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_signup_date_month_by = Window.partitionBy("country", F.date_trunc("month", "signup_date")).orderBy(F.col("birth_year").desc())

valid_listeners = (
    df_listeners
    .filter((F.col("birth_year") > 1967) & (F.size(F.col("interests")) >= 1))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        F.add_months(F.col("signup_date"), 12).alias("signup_date_plus_12m"),
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
    )
)

with_windows = (
    valid_listeners
    .select(
        "*",
        F.first("birth_year").over(w_all_by_signup_date_plus_12m).alias("first_birth_year"),
        F.count(F.lit(1)).over(w_signup_date_month).alias("rows_in_signup_date_month"),
        F.min("birth_year").over(w_signup_date_month).alias("signup_date_month_min_birth_year"),
        F.sum("birth_year").over(w_signup_date_month).alias("signup_date_month_sum_birth_year"),
        F.last("signup_date_plus_12m", ignorenulls=True).over(w_all_by_signup_date_plus_12m_2).alias("last_known_signup_date_plus_12m"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_signup_date_month), F.lit(0)), 4).alias("birth_year_share_of_signup_date_month"),
        F.datediff(F.col("signup_date"), F.lag("signup_date").over(w_all_by_signup_date_plus_12m_3)).alias("days_since_prev_signup_date"),
        F.first("signup_date").over(w_signup_date_year_country_by).alias("first_signup_date"),
        F.rank().over(w_country_signup_date_month_by).alias("birth_year_rank"),
        F.last("birth_year").over(w_all_by_signup_date_plus_12m).alias("last_birth_year"),
    )
)

result = (
    with_windows
    .filter(F.col("birth_year_share_of_signup_date_month") > 0.2)
    .select(
        "id",
        "country",
        "signup_date",
        "birth_year",
        "country_clean",
        "first_birth_year",
        "rows_in_signup_date_month",
        "signup_date_month_min_birth_year",
        "birth_year_share_of_signup_date_month",
        "days_since_prev_signup_date",
        "birth_year_rank",
        "last_birth_year",
    )
    .orderBy("country", F.col("id").desc())
)
