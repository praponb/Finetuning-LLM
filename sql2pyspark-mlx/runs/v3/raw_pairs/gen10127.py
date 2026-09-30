from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("method") == "cash")

summary = (
    filtered_payments
    .groupBy("method")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    summary
    .select("method", "amount_count")
    .orderBy(F.col("amount_count").desc(), "method")
)
