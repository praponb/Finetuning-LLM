from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.when(F.col("o.discount") >= 0.44, "high").when(F.col("o.discount") >= 0.12, "mid").otherwise("low").alias("discount_bucket"),
        F.col("o.customer_id"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), "discount_bucket", "customer_id")
    .limit(10)
)
