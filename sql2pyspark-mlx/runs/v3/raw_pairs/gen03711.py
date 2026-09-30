from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount").between(770, 1530) | (F.col("oi.unit_price") < 290))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("o.customer_id"),
        F.col("o.discount"),
    )
    .orderBy(F.col("discount").desc())
)
