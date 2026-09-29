from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 20))
    .groupBy("method", "status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1350)
)
