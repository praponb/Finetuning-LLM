from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("shipping_fee_whole", F.floor("shipping_fee"))
)
