from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "paypal")
    .groupBy("method")
    .agg(F.avg("amount").alias("avg_amount"))
)
