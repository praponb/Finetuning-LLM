from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.customer_id").isNull())
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.when(F.col("s.status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .orderBy("cancelled_count")
)
