from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id") == 24)
    .select(
        F.col("oi.order_id"),
        F.col("p.launch_date"),
        F.col("o.status"),
        F.when(F.col("p.brand") == "Acme", 1).otherwise(0).alias("is_acme"),
    )
)
