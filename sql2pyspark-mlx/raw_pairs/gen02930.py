from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .groupBy(
        F.year(F.col("orders.order_date")).alias("order_year"),
        F.col("orders.status"),
    )
    .agg(
        F.avg(F.col("order_items.quantity")).alias("avg_quantity"),
        F.min(F.col("order_items.unit_price")).alias("min_unit_price"),
    )
    .orderBy("min_unit_price")
)
