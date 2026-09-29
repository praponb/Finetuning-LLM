from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.count("units").alias("units_count"))
    .orderBy(F.col("sale_quarter").desc())
    .limit(1)
)
