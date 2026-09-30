from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("method").isin(["promptpay", "bank_transfer", "card"]))

status_summary = (
    filtered_payments
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
)

result = (
    status_summary
    .select("status", "pct_card")
    .orderBy(F.col("pct_card").desc())
)
