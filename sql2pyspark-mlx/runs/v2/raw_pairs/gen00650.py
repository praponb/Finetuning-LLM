from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.hour("paid_at").between(10, 13) & (F.col("method") == "paypal"))
)

status_summary = (
    src
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    status_summary
    .select("status", "avg_amount")
    .orderBy("status", F.col("avg_amount").desc())
)
