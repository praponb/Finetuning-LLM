from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg((F.max("price") - F.min("price")).alias("price_range"))
    .orderBy(F.col("price_range").desc())
)
