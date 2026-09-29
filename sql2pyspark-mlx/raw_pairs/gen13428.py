from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.unit_price") > 160)
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("o.discount"),
        F.col("o.status"),
    )
    .orderBy("product_name", F.col("order_id").desc())
)
