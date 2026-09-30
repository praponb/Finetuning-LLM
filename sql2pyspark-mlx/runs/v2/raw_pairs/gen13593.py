from pyspark.sql import functions as F

src = (
    df_products
    .filter(~F.col("product_name").like("%Mak%") & (F.col("brand") == "Stark"))
    .withColumn("price_int", F.col("price").cast("int"))
)

agg = (
    src
    .groupBy("category", "brand")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = agg.orderBy("category")
