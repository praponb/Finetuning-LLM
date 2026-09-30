from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.unit_price") < 220)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("p.cost"),
        F.col("p.price"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
