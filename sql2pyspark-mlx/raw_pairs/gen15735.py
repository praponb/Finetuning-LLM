from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("revenue") > 2900)

summary = (
    recent_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"), "product_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    summary
    .filter(F.col("units_count") >= 10)
    .select("sale_quarter", "product_id", "units_count")
    .orderBy(F.col("units_count").desc())
)
