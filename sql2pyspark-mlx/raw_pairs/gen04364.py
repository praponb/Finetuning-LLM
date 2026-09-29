from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Lap%"))
    .groupBy("category")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 6)
    .select("category", "row_count")
    .orderBy("row_count")
)
