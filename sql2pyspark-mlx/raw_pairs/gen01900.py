from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") > "2025-02-01"))
    .select("*", F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"))
)

result = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.segment").alias("segment"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("src.monthly_fee_filled")).alias("avg_monthly_fee_filled"),
    )
)
