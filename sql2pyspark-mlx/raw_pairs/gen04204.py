from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "card"]))
    .groupBy(F.hour("paid_at").alias("paid_hour"), "status")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") >= 1260)
    .orderBy("avg_amount", "paid_hour")
)
