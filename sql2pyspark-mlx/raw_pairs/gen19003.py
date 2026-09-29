from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([9, 22, 35]) & (F.col("start_date").between("2024-02-01", "2025-09-01") | F.col("customer_id").isNull()))
    .groupBy("customer_id", "plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .orderBy(F.col("plan").desc())
)
