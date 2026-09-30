from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"), "brand")
    .agg(F.count("*").alias("row_count"))
)
