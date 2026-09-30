from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-09-01", "2024-12-30") & F.col("product_id").isin([2, 3, 8]))
)

summary = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    summary
    .filter(F.col("stddev_revenue") < 50)
    .select("product_id", "stddev_revenue", "revenue_range")
    .orderBy(F.col("stddev_revenue").desc())
)
