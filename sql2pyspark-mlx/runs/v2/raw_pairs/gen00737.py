from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("status") == "FAILED")

agg = (
    valid_payments
    .groupBy("status")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    agg
    .filter(F.col("amount_count") >= 3)
    .orderBy(F.col("amount_count").desc(), "status")
    .limit(5)
)
