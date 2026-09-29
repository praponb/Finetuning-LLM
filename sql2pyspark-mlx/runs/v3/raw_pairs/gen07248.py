from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "bank_transfer", "card"]) & F.col("method").isin(["card", "promptpay", "cash"]))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

agg = (
    recent_payments
    .groupBy("method")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    agg
    .filter(F.col("avg_amount") < 590)
    .select("method", "avg_amount")
)
