from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.order_id").isin([20, 30]))
    .groupBy(
        F.when(F.col("o.discount") >= 0.36, "high").when(F.col("o.discount") >= 0.14, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"))
    .filter(F.col("revenue") > 42)
    .orderBy("revenue", F.col("discount_bucket").desc())
    .limit(20)
)
