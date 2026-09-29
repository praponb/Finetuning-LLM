from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity").between(120, 212))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("t.quantity_count"),
        F.col("t.max_quantity"),
    )
    .orderBy(F.col("quantity_count").desc())
)
