from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-03-12").cast("date")) & (F.col("balance") > 45000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        (F.datediff(F.current_date(), "opened_date") / 365).alias("years_since_opened_date"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("years_since_opened_date", 0.95).alias("years_since_opened_date_p95"),
        F.percentile_approx("years_since_opened_date", 0.5).alias("years_since_opened_date_p50"),
    )
    .orderBy(F.col("years_since_opened_date_p95").asc_nulls_last())
)
