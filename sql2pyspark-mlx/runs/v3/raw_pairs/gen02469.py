from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isin([5, 8, 40]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("o.amount"),
        (F.col("o.discount") / F.nullif(F.col("o.shipping_fee"), F.lit(0))).alias("discount_per_shipping_fee"),
    )
)
