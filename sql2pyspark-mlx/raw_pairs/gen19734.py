from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("discount_plus_shipping_fee", (F.col("discount") + F.col("shipping_fee")))
    .orderBy("id")
)
