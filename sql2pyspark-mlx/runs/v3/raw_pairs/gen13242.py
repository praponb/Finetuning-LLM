from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("sale_date").between("2024-06-13", "2025-08-01"))

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    agg
    .filter(F.col("num_store_sales") > 4)
    .orderBy(F.col("num_store_sales").desc())
)
