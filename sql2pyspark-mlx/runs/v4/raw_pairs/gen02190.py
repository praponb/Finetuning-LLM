from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("o.status").isin(["CANCELLED", "RETURNED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
    .filter(F.col("max_quantity") >= 5.7)
    .orderBy("max_quantity", "status_group")
    .limit(1)
)
