from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.launch_date") > "2024-02-01")
    .groupBy(F.col("o.channel"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.max(F.col("oi.quantity")).alias("max_quantity"),
    )
)
