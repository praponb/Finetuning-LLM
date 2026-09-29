from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Home")
    .groupBy("category")
    .agg(F.min("price").alias("min_price"))
    .orderBy("min_price", "category")
    .limit(10)
)
