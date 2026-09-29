from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.avg("amount").alias("avg_amount"))
)
