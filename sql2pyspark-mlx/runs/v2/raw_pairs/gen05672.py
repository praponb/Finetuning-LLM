from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-12-08", "2025-04-01"))
    .groupBy("store_id", "product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)
