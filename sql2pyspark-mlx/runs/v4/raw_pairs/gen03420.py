from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 700) | F.col("supplier_id").isNull())
    .withColumn("price_whole", F.ceil("price"))
    .orderBy(F.col("id").desc())
)
