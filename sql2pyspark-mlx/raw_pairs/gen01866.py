from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") < "2025-02-17") & (F.col("method") == "paypal"))
    .groupBy("method", F.hour("paid_at").alias("paid_hour"))
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy("method", "paid_hour")
    .limit(3)
)
