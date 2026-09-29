from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2025)
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 6)
    .select("store_id", "row_count")
    .orderBy(F.col("store_id").desc(), F.col("row_count").desc())
)
