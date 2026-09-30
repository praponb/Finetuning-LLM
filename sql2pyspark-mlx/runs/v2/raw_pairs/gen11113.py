from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.stock_qty") > 259) | (F.col("o.shipping_fee") <= 36))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.concat_ws("/", F.col("o.status"), F.col("o.channel")).alias("status_channel_key"),
    )
)
