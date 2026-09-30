from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") >= 5)
    .select("store_id", "min_sale_date", "revenue_count")
)
