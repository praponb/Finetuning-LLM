from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-05-01", "2025-07-01"))
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("sale_quarter").desc())
)
