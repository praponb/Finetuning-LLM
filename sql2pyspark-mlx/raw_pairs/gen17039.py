from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 62, "high").when(F.col("c.age") >= 29, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.avg(F.col("o.discount")).alias("avg_discount"),
        F.max(F.col("o.order_date")).alias("latest_order_date"),
    )
)
