from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Stark"]))
    .select("id", "product_name", F.explode("tags").alias("tag_name"))
)
