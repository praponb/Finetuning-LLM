from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.discount"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.trim(F.col("p.product_name")).alias("clean_product_name"),
    )
    .orderBy("clean_product_name")
)
