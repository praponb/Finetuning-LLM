from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.amount") >= 1630, "high").when(F.col("o.amount") >= 590, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
)
