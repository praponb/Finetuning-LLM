from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_product_in_home_life_by_pr = Window.partitionBy("n_product_in_home_life").orderBy("premium_to_start_date_quarter_ratio")
w_end_date_year_day_month_n_pr = Window.partitionBy(F.month("end_date_year_day"), "n_product_in_home_life").orderBy(F.col("premium_to_start_date_quarter_ratio").desc_nulls_first(), "end_date_year_day")
w_end_date_year_day_month_n_pr_2 = Window.partitionBy(F.month("end_date_year_day"), "n_product_in_home_life").orderBy(F.col("premium_to_start_date_quarter_ratio").desc_nulls_first(), "end_date_year_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_policys = (
    df_policies
    .filter((~F.col("status").isin("EXPIRED", "LAPSED")) & (F.col("start_date") >= F.lit("2024-04-03").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("year", F.col("end_date")).alias("end_date_year"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
    )
)

summary = (
    recent_policys
    .groupBy(F.to_date("end_date_year").alias("end_date_year_day"))
    .agg(
        F.sum(F.when(F.col("status") != "EXPIRED", F.col("premium")).otherwise(0)).alias("premium_status_expired"),
        F.round(F.sum("premium") / F.nullif(F.sum("start_date_quarter"), F.lit(0)), 3).alias("premium_to_start_date_quarter_ratio"),
        F.count(F.when(F.col("product").isin("HOME", "LIFE"), 1)).alias("n_product_in_home_life"),
        F.min("end_date").alias("first_end_date"),
    )
)

metrics = (
    summary
    .select(
        "*",
        F.rank().over(w_n_product_in_home_life_by_pr).alias("premium_to_start_date_quarter_ratio_rank"),
        (F.col("premium_status_expired") - F.lag("premium_status_expired").over(w_end_date_year_day_month_n_pr)).alias("premium_status_expired_change"),
        F.last("first_end_date").over(w_end_date_year_day_month_n_pr_2).alias("last_first_end_date"),
    )
)

result = (
    metrics
    .filter(F.col("last_first_end_date") < F.lit("2026-10-23").cast("date"))
    .select(
        "end_date_year_day",
        "premium_status_expired",
        "premium_to_start_date_quarter_ratio",
        "n_product_in_home_life",
        "premium_to_start_date_quarter_ratio_rank",
        "premium_status_expired_change",
        (F.floor(F.col("premium_status_expired") / 5000) * 5000).alias("premium_status_expired_bucket"),
        F.date_trunc("month", F.col("last_first_end_date")).alias("last_first_end_date_month"),
    )
    .orderBy(F.col("premium_status_expired_change").desc(), "end_date_year_day")
    .limit(20)
)
