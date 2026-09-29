from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-08-01")
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("t.units_count"),
        F.col("t.avg_revenue"),
    )
    .orderBy(F.col("units_count").desc(), "id")
    .limit(3)
)
