from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "USB-C HUB")
    .select(F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc(), "label")
)
