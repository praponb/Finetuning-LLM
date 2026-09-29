from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .withColumn("discount_int", F.col("discount").cast("int"))
    .orderBy("status")
)
