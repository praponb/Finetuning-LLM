from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("revenue") >= 1900)
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("P.avg_units"),
        F.col("P.min_revenue"),
    )
    .orderBy("category")
)
