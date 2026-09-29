from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(
        F.when(F.col("o.status").isin(["PENDING", "COMPLETED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"))
)
