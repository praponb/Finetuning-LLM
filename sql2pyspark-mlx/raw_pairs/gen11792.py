from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("o.channel").isNull())
    .groupBy(
        F.when(F.col("s.sqft") >= 16701, "high").when(F.col("s.sqft") >= 4831, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
    .orderBy("sqft_bucket")
)
