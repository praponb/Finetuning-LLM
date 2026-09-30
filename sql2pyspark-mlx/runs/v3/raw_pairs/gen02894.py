from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("p.cost"),
        F.upper(F.trim(F.col("p.product_name"))).alias("product_name_normalized"),
    )
    .orderBy(F.col("id").desc(), F.col("shipping_fee").desc())
)
