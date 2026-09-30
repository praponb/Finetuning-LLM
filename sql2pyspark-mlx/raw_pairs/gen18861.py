from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(~((F.col("revenue") >= 2800) & F.col("product_id").isin([2, 6])))
)

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    agg
    .filter(F.col("revenue_count") >= 6)
    .select("product_id", "avg_units", "revenue_count")
)
