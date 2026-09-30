from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.amount") < 920)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("o.shipping_fee"),
    )
    .orderBy("launch_date")
)
