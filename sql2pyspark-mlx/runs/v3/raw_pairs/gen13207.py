from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.upper(F.col("c.name")).like("%OB %"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("s.status") == "CANCELLED", F.col("s.monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
)
