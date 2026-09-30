from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_min_premium_full = Window.partitionBy("product").orderBy(F.col("min_premium").asc_nulls_last(), "start_date_month", "product").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_min_premium = Window.partitionBy("product").orderBy(F.col("min_premium").asc_nulls_last(), "start_date_month", "product")
w_product = Window.partitionBy("product")

valid = (
    df_policies
    .filter(F.col("premium") >= 2111.3)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

totals = (
    valid
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "product",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.sum("premium").alias("total_premium"),
        F.min("premium").alias("min_premium"),
    )
)

ranked = (
    totals
    .select(
        "start_date_month",
        "product",
        "pct_status_is_not_null",
        "total_premium",
        F.first("total_premium").over(w_product_by_min_premium_full).alias("first_total_premium"),
        (F.col("min_premium") - F.lag("min_premium").over(w_product_by_min_premium)).alias("min_premium_change"),
        F.min("pct_status_is_not_null").over(w_product).alias("product_min_pct_status_is_not_null"),
        F.rank().over(w_product_by_min_premium).alias("min_premium_rank"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        (F.col("start_date_month") + F.expr("INTERVAL 30 MINUTES")).alias("start_date_month_plus_30m"),
    )
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.avg("product_min_pct_status_is_not_null").alias("overall_avg_product_min_pct_status_is_not_null")))
    .filter(F.col("product_min_pct_status_is_not_null") > F.col("overall_avg_product_min_pct_status_is_not_null"))
    .drop("overall_avg_product_min_pct_status_is_not_null")
)

annotated = (
    filtered
    .select(
        "start_date_month",
        "product",
        "pct_status_is_not_null",
        "total_premium",
        "min_premium_change",
        "product_min_pct_status_is_not_null",
        "min_premium_rank",
        (F.col("start_date_month") + F.expr("INTERVAL 7 DAYS")).alias("start_date_month_plus_7d"),
        F.when(F.col("min_premium_rank") >= 8, "HIGH").when(F.col("min_premium_rank") >= 3, "MEDIUM").otherwise("LOW").alias("min_premium_rank_tier"),
        F.least(F.col("min_premium_rank"), F.col("first_total_premium")).alias("least_min_premium_rank_first_total_premium"),
    )
)

result = (
    annotated
    .select(
        "start_date_month",
        "product",
        "pct_status_is_not_null",
        "total_premium",
        "min_premium_change",
        "product_min_pct_status_is_not_null",
        "min_premium_rank",
        "start_date_month_plus_7d",
        "min_premium_rank_tier",
        "least_min_premium_rank_first_total_premium",
        F.round(F.col("total_premium") / F.nullif(F.col("pct_status_is_not_null"), F.lit(0)), 3).alias("total_premium_per_pct_status_is_not_null"),
    )
    .orderBy(
        F.col("min_premium_rank_tier").asc(),
        F.col("least_min_premium_rank_first_total_premium").desc_nulls_first(),
        "start_date_month",
        "product",
    )
    .limit(5)
)
