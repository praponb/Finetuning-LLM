from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.status") == "ACTIVE")
    .groupBy(F.col("c.country"))
    .agg(
        F.count("*").alias("row_count"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") >= 100)
)
