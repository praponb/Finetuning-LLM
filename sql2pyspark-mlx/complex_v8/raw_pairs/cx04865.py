from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_group_signup_date_mo = Window.partitionBy("country_group", F.date_trunc("month", "signup_date"))
w_all_by_signup_date_moving_2 = Window.orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w_all_by_signup_date_ties = Window.orderBy("signup_date")
w_country_by_rows_in_country_g = Window.partitionBy("country").orderBy("rows_in_country_group_signup_date_month")
w_country_group_by_birth_year = Window.partitionBy("country_group").orderBy("birth_year", "id").rowsBetween(-1, Window.currentRow)
w_signup_date_dense_rank_count = Window.partitionBy("signup_date_dense_rank", "country").orderBy(F.col("days_since_signup_date_avg_3").asc_nulls_last(), "id")
w_country_group_by_birth_year_2 = Window.partitionBy("country_group").orderBy("birth_year", "id")
w_birth_year_quintile_by_days = Window.partitionBy("birth_year_quintile").orderBy("days_since_signup_date_avg_3_quintile", "id")
w_birth_year_quintile_by_birth = Window.partitionBy("birth_year_quintile").orderBy(F.col("birth_year").desc(), "id").rowsBetween(-6, -1)
w_birth_year_quintile_by_birth_2 = Window.partitionBy("birth_year_quintile").orderBy(F.col("birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_listeners = (
    df_listeners
    .filter(F.col("signup_date").between(F.lit("2024-08-02").cast("date"), F.lit("2025-05-05").cast("date")))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
        F.when(F.col("country").isin("BR", "TH"), "group_a").when(F.col("country") == "JP", "group_b").otherwise("other").alias("country_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

ranked = (
    base_listeners
    .select(
        "*",
        F.count(F.lit(1)).over(w_country_group_signup_date_mo).alias("rows_in_country_group_signup_date_month"),
        F.round(F.avg("days_since_signup_date").over(w_all_by_signup_date_moving_2), 2).alias("days_since_signup_date_avg_3"),
        F.dense_rank().over(w_all_by_signup_date_ties).alias("signup_date_dense_rank"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.round(F.cume_dist().over(w_country_by_rows_in_country_g), 3).alias("rows_in_country_group_signup_date_month_cume_dis"),
        F.max("birth_year").over(w_country_group_by_birth_year).alias("birth_year_max_2"),
        F.ntile(5).over(w_signup_date_dense_rank_count).alias("days_since_signup_date_avg_3_quintile"),
        F.ntile(5).over(w_country_group_by_birth_year_2).alias("birth_year_quintile"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.lag("days_since_signup_date_avg_3", 3).over(w_birth_year_quintile_by_days).alias("prev3_days_since_signup_date_avg_3"),
        F.max("days_since_signup_date_avg_3").over(w_birth_year_quintile_by_birth).alias("days_since_signup_date_avg_3_max_6_prior"),
        F.last("birth_year_max_2", ignorenulls=True).over(w_birth_year_quintile_by_birth_2).alias("last_known_birth_year_max_2"),
    )
)

totals = (
    with_trends_2
    .groupBy(
        "birth_year_quintile",
        F.date_format("signup_date", "yyyy-MM").alias("signup_date_ym"),
    )
    .agg(
        F.round(F.sum("days_since_signup_date_avg_3_max_6_prior"), 2).alias("total_days_since_signup_date_avg_3_max_6_prior"),
        F.round(F.avg("days_since_signup_date_avg_3_quintile"), 2).alias("avg_days_since_signup_date_avg_3_quintile"),
    )
)

qualified = (
    totals
    .crossJoin(totals.agg(F.max("birth_year_quintile").alias("overall_max_birth_year_quintile")))
    .filter(F.col("birth_year_quintile") < F.col("overall_max_birth_year_quintile"))
    .drop("overall_max_birth_year_quintile")
)

labeled = (
    qualified
    .select(
        "*",
        F.substring(F.trim(F.col("signup_date_ym")), 1, 3).alias("signup_date_ym_prefix"),
        F.substring(F.trim(F.col("signup_date_ym")), 1, 4).alias("signup_date_ym_prefix_2"),
        F.coalesce(F.col("birth_year_quintile"), F.lit(0)).alias("birth_year_quintile_filled"),
    )
)

result = (
    labeled
    .select(
        "birth_year_quintile",
        "signup_date_ym",
        "total_days_since_signup_date_avg_3_max_6_prior",
        "avg_days_since_signup_date_avg_3_quintile",
        "signup_date_ym_prefix_2",
        "birth_year_quintile_filled",
        F.concat_ws("-", F.col("signup_date_ym"), F.col("signup_date_ym_prefix")).alias("signup_date_ym_signup_date_ym_prefix_label"),
    )
    .orderBy("birth_year_quintile")
)
