from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.count("*").alias("order_item_count"),
    )
)
