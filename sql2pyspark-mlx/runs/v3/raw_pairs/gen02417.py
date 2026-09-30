from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.price") < 1160)
    .select(
        F.col("oi.unit_price"),
        F.col("p.launch_date"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("amount").desc())
)
