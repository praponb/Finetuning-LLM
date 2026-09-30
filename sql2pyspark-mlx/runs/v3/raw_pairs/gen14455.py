from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.end_date") >= "2025-05-01")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("s.status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .orderBy(F.col("segment").desc())
    .limit(5)
)
