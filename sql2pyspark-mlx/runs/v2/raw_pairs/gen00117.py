from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("o.channel").isin(["partner", "mobile", "store"]) | F.col("p.price").between(720, 1140))
    .select(
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
)
