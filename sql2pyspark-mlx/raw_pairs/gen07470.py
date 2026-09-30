from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price").between(180, 390))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.cost"),
        F.col("p.category"),
        F.col("t.avg_quantity"),
        F.col("t.avg_unit_price"),
    )
)
