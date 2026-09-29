from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "bank_transfer", "card"]) & F.col("method").isin(["card", "promptpay", "cash"]))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

agg = (
    recent_payments
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
)

result = (
    agg
    .filter(F.col("cnt") < 4)
    .select("status", "cnt")
)
