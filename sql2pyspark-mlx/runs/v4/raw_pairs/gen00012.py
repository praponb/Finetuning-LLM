from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 23)
    .withColumn("shipping_fee_rounded", F.round("shipping_fee", 2))
    .withColumn("amount_whole", F.ceil("amount"))
    .orderBy(F.col("discount").desc(), "id")
    .limit(10)
)
