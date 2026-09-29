from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer"]))
    .withColumn("method_prefix", F.substring("method", 1, 4))
)

method_summary = (
    valid_payments
    .groupBy("method", F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    method_summary
    .select("method", "paid_day", "avg_amount", "amount_range")
    .orderBy("amount_range", "method", "paid_day")
    .limit(5)
)
