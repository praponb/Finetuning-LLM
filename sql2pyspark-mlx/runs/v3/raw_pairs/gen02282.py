from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.col("launch_date") > "2023-07-29")
    .withColumn("product_name_length", F.length("product_name"))
)

agg = (
    src
    .groupBy("brand")
    .agg((F.max("price") - F.min("price")).alias("price_range"))
)

result = (
    agg
    .filter(F.col("price_range") > 21)
    .select("brand", "price_range")
    .orderBy("brand", "price_range")
)
