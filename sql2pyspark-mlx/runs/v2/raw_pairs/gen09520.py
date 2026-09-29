from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.max("price").alias("max_price"),
        F.count("price").alias("price_count"),
    )
    .filter(F.col("price_count") > 6)
    .select("category", "max_price", "price_count")
    .orderBy(F.col("price_count").desc())
)
