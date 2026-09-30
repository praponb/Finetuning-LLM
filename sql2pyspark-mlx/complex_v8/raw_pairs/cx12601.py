from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_signup_date_month = Window.partitionBy(F.month("signup_date"))
w_country_by_birth_year = Window.partitionBy("country").orderBy("birth_year", "id")
w_all_by_signup_date_ties = Window.orderBy("signup_date")
w_all_by_signup_date_running = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_signup_date_year = Window.partitionBy(F.year("first_known_signup_date")).orderBy(F.col("signup_date").asc_nulls_last(), "id")
w_all_by_signup_date_month_max = Window.orderBy(F.col("signup_date_month_max_birth_year").desc_nulls_first(), "id")
w_all_by_first_known_signup_da = Window.orderBy(F.col("first_known_signup_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_signup_date_month_max_2 = Window.orderBy(F.col("signup_date_month_max_birth_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_listeners = (
    df_listeners
    .filter(F.col("country") == "KR")
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

with_trends = (
    filtered_listeners
    .select(
        "*",
        F.min("days_since_signup_date").over(w_signup_date_month).alias("signup_date_month_min_days_since_signup_date"),
        F.round(F.cume_dist().over(w_country_by_birth_year), 3).alias("birth_year_cume_dist"),
        F.dense_rank().over(w_country_by_birth_year).alias("birth_year_dense_rank"),
        F.max("birth_year").over(w_signup_date_month).alias("signup_date_month_max_birth_year"),
        F.rank().over(w_all_by_signup_date_ties).alias("signup_date_rank"),
        F.first("signup_date", ignorenulls=True).over(w_all_by_signup_date_running).alias("first_known_signup_date"),
        F.ntile(3).over(w_country_by_birth_year).alias("birth_year_tercile"),
        F.count(F.lit(1)).over(w_signup_date_month).alias("rows_in_signup_date_month"),
    )
)

enriched = (
    with_trends
    .filter(F.col("rows_in_signup_date_month") <= 17)
    .select(
        "*",
        F.upper(F.trim(F.col("country"))).alias("country_clean"),
        F.array_join(F.array_sort(F.col("interests")), ",").alias("interests_text"),
    )
)

with_trends_2 = (
    enriched
    .select(
        "*",
        F.lag("birth_year_tercile", 3).over(w_first_known_signup_date_year).alias("prev3_birth_year_tercile"),
        F.ntile(10).over(w_all_by_signup_date_month_max).alias("signup_date_month_max_birth_year_decile"),
        F.round(F.avg("days_since_signup_date").over(w_all_by_first_known_signup_da), 2).alias("days_since_signup_date_avg_4"),
        F.nth_value("birth_year_tercile", 2).over(w_all_by_signup_date_month_max_2).alias("second_birth_year_tercile"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        "days_since_signup_date",
        "birth_year_cume_dist",
        "signup_date_month_max_birth_year",
        "signup_date_rank",
        "first_known_signup_date",
        "rows_in_signup_date_month",
        "country_clean",
        "interests_text",
        "signup_date_month_max_birth_year_decile",
        "second_birth_year_tercile",
        F.when(F.col("country").isin("JP", "TH"), "group_a").when(F.col("country") == "UK", "group_b").otherwise("other").alias("country_group"),
        F.quarter(F.col("signup_date")).alias("signup_date_quarter"),
    )
    .orderBy("rows_in_signup_date_month")
)
