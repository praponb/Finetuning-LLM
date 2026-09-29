from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Fashion")
    .agg(F.count("*").alias("product_count"))
)
