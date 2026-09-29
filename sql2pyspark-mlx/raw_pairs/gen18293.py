from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 39) & (F.col("amount") >= 830))
    .withColumn("clean_status", F.trim("status"))
    .orderBy("amount")
)
