from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(~((F.col("o.shipping_fee") > 34) & (F.col("o.shipping_fee") < 39)))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("o.discount"),
        F.col("o.amount"),
    )
)
