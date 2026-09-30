from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("p.price"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
    )
    .orderBy("price")
)
