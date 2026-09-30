from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("sale_date").between("2023-05-01", "2024-10-13"))

agg = (
    recent_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    agg
    .filter(F.col("store_sale_count") < 9)
    .select("store_id", "store_sale_count")
)
