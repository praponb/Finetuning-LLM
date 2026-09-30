from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 230) & (F.col("order_id").isin([5, 11, 37]) | (F.col("unit_price") > 300)))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
    .orderBy(F.col("id").desc(), "product_id")
)
