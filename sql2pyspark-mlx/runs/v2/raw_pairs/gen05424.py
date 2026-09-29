from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 2400)
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.count("*").alias("num_store_sales"))
    .orderBy("sale_year", F.col("num_store_sales").desc())
)
