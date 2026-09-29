from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Fashion", "Electronics"]))
    .select("id", "price", F.explode("tags").alias("tag_name"))
)
