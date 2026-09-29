from pyspark.sql import functions as F

recent_payments = df_payments.filter((F.col("method") == "promptpay") & (F.col("amount") <= 510))

status_group_summary = (
    recent_payments
    .groupBy(
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    status_group_summary
    .filter(F.col("amount_count") > 8)
    .orderBy(F.col("avg_amount").desc(), "status_group")
    .limit(10)
)
