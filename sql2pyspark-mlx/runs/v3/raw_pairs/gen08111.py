from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("store_id").isin([1, 3, 4, 5]) & (F.col("units") != 39))

store_sales_stats = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("num_store_sales") > 9)
    .select("store_id", "unique_stores", "num_store_sales")
    .orderBy("unique_stores", F.col("num_store_sales").desc())
)
