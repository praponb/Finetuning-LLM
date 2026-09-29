from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("amount").isNull() & (F.hour("paid_at").between(7, 10) | F.col("method").isin(["cash", "promptpay"])))
)

summary = (
    filtered_payments
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1530, "high").when(F.col("amount") >= 210, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = summary.select("status", "amount_bucket", "avg_amount")
