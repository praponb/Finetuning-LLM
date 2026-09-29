from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-09-29")
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("total_amount")
)
