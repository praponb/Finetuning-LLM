from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit").between(9900, 13700))
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("subscription_count"),
        F.sum(F.when(F.col("s.status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
    )
    .orderBy("active_count", "segment")
    .limit(10)
)
