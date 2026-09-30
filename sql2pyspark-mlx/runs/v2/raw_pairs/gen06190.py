from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.launch_date") > "2023-11-01")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("p.launch_date"),
    )
    .orderBy("id")
)
