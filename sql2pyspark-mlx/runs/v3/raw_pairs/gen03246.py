from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("oi.product_id").isin([7, 8]))
    .groupBy(
        F.when(F.col("o.amount") >= 1530, "high").when(F.col("o.amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") >= 50)
    .orderBy(F.col("avg_unit_price").desc())
)
