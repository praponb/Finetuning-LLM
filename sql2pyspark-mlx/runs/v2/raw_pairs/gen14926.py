from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("supplier_id").isin([4, 18, 27, 36]))
    .withColumn("brand_length", F.length("brand"))
)

agg = (
    filtered_products
    .groupBy("supplier_id", "brand")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = agg.filter(F.col("avg_cost") < 470)
