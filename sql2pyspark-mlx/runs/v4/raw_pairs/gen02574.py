from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.unit_price") >= 230)
    .select(
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("oi.quantity"),
    )
)
