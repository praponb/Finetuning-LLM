from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.order_id").isin([11, 27, 36]) | F.col("oi.unit_price").between(220, 370))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("o.shipping_fee"),
        F.round((F.col("p.cost") * 1.1), 2).alias("adjusted_cost"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
