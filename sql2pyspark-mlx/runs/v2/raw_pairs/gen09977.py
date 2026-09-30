from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count("*").alias("total_count"),
        F.count("price").alias("price_count"),
    )
    .filter(F.col("price_count") >= 3)
    .drop("price_count")
    .orderBy("category")
)
