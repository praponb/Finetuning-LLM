from pyspark.sql import functions as F

result = (
    df_products
    .agg((F.max("price") - F.min("price")).alias("price_range"))
    .orderBy("price_range")
)
