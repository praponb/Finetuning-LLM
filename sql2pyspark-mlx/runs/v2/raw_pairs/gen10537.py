from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.product_id").isNull())
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.amount"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
)
