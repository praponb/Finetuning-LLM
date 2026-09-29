from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter((F.col("units") < 29) & F.month("sale_date").isin([1, 6, 8]) & (F.col("revenue") > 3800))
)

store_id_summary = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    store_id_summary
    .select("store_id", "avg_units", "min_revenue")
    .orderBy(F.col("store_id").desc())
)
