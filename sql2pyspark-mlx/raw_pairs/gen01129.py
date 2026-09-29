from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.revenue_count") > 6)
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("t.units_range"),
        F.col("t.revenue_count"),
    )
    .orderBy("id", "revenue_count")
    .limit(10)
)
