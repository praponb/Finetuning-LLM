from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isNotNull())
    .groupBy(F.col("c.segment"))
    .agg(F.count(F.when(F.col("s.status") == "CANCELLED", 1)).alias("cancelled_count"))
)
