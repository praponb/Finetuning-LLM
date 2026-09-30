from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .select(
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("o.customer_id"),
        (F.col("o.amount") - F.col("o.shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy("price", F.col("order_id").desc())
)
