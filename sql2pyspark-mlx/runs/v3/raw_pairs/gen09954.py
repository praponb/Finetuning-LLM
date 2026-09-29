from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 59, "high").when(F.col("c.age") >= 42, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.channel") == "web", F.col("o.shipping_fee")).otherwise(0)).alias("web_shipping_fee"),
    )
    .filter(F.col("web_shipping_fee") >= 38)
    .orderBy(F.col("web_shipping_fee").desc(), "age_bucket")
)
