from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.count("units").alias("units_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.min_revenue") > 3000)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("t.min_revenue"),
        F.col("t.units_count"),
    )
    .orderBy("min_revenue")
)
