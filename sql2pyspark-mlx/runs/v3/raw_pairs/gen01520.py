from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1530)
    .withColumn("shipping_fee_times_discount", (F.col("shipping_fee") * F.col("discount")))
)
