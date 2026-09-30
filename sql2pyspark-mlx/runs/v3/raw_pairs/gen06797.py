from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(F.count("amount").alias("amount_count"))
)
