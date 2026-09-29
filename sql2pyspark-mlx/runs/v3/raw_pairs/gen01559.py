from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 29)
    .groupBy("status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1030)
)
