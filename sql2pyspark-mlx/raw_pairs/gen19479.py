from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Chiang Mai")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.sqft") >= 12566, "high").when(F.col("s.sqft") >= 7659, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "CANCELLED", F.col("o.discount")).otherwise(0)).alias("cancelled_discount"),
    )
    .orderBy(F.col("cancelled_discount").desc())
)
