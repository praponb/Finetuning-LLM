from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("method").isin(["promptpay", "card", "cash"]))

method_summary = (
    recent_payments
    .groupBy("method", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    method_summary
    .filter(F.col("amount_range") > 23)
    .select("method", "paid_month", "amount_range")
    .orderBy(F.col("method").desc())
)
