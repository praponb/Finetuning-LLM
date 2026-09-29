from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 1490)
    .select("paid_hour", "avg_amount")
)
