from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isNotNull())
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.amount") >= 1580, "high").when(F.col("o.amount") >= 540, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
)
