from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.max("amount").alias("max_amount"))
)
