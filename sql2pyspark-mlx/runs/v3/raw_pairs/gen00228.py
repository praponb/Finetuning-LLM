from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.city").isin(["Berlin", "London"]))
    .groupBy(
        F.col("c.is_active"),
        F.when(F.col("c.age") >= 56, "high").when(F.col("c.age") >= 34, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "RETURNED", F.col("o.shipping_fee")).otherwise(0)).alias("returned_shipping_fee"),
        F.sum(F.when(F.col("o.status") == "CANCELLED", F.col("o.shipping_fee")).otherwise(0)).alias("cancelled_shipping_fee"),
    )
)
