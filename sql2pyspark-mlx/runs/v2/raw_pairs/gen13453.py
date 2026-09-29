from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("revenue") <= 1600)
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("units").alias("min_units"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("t.avg_units"),
        F.col("t.min_units"),
    )
    .orderBy(F.col("price").desc())
)
