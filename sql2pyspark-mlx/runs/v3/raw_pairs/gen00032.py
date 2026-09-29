from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("units").between(16, 22))

agg = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)

result = agg.orderBy("row_count")
