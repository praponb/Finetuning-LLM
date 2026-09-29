from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"), "product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("units_count") >= 1)
)
