from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 27) & (F.year("sale_date") == 2024))
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy(F.col("store_sale_count").desc())
)
