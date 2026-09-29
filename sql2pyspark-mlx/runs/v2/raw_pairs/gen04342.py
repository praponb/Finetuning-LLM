from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("p.brand"),
    )
    .orderBy("brand")
)
