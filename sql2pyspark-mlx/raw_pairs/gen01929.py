from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([23, 29]))
    .withColumn("unit_price_whole", F.floor("unit_price"))
    .orderBy(F.col("id").desc())
)
