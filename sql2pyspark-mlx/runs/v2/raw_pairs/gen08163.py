from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("shipping_fee_times_discount", (F.col("shipping_fee") * F.col("discount")))
)
