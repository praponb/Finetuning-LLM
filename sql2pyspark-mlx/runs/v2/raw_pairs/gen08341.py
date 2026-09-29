from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%IRE%"))
    .select(F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.count("*").alias("product_count"))
)
