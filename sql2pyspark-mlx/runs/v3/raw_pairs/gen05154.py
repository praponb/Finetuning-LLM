from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1200)
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy("sale_year", F.col("store_sale_count").desc())
    .limit(50)
)
