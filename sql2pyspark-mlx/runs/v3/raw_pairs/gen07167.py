from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.total_quantity") > 14)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("t.total_quantity"),
        F.col("t.unit_price_count"),
    )
    .orderBy(F.col("total_quantity").desc(), "id")
    .limit(50)
)
