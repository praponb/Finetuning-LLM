from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Fashion", "Electronics"]))
    .agg(F.countDistinct("price").alias("unique_categorys"))
)
