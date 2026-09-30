from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1540)
    .groupBy(F.hour("paid_at").alias("paid_hour"), "status")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .orderBy("paid_hour")
)
