from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "status")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") >= 460)
    .orderBy("status")
)
