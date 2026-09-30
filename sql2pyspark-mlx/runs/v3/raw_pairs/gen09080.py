from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id").isin([6, 11, 21, 28]))
    .groupBy("product_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("ProductTotals"), F.col("products.id") == F.col("ProductTotals.product_id"), "inner")
    .filter(F.col("ProductTotals.avg_unit_price") > 170)
    .select(
        F.col("products.id"),
        F.col("products.price"),
        F.col("products.cost"),
        F.col("ProductTotals.avg_unit_price"),
    )
    .orderBy(F.col("price").desc())
)
