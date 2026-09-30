from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Books"]))
    .withColumn("adjusted_price", F.round((F.col("price") * 0.9), 2))
    .orderBy(F.col("category").desc(), "brand")
)
