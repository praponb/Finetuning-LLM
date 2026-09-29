from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2025-01-01")
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 51)
    .orderBy("product_id")
)
