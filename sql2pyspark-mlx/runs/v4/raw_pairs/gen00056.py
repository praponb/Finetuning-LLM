from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1330) | (F.col("amount") <= 1420))
    .groupBy("status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 480)
)
