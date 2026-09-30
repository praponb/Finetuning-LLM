from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id") == 10)
    .groupBy(F.col("o.status"))
    .agg(
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("quantity_range").desc(), F.col("unique_products").desc())
)
