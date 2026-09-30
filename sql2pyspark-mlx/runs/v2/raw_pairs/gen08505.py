from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 8]))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
    .orderBy(F.col("id").desc())
)
