from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("store_id").desc())
)
