from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(((F.col("sale_date") <= "2024-11-20") & (F.col("sale_date") > "2025-09-04")) | (F.col("store_id") == 5))
)

store_id_summary = (
    src
    .groupBy("store_id", "product_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = store_id_summary.filter(F.col("min_revenue") < 1100)
