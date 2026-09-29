from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .withColumn("amount_times_discount", (F.col("amount") * F.col("discount")))
)
