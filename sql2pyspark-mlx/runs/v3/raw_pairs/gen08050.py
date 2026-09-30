from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
    )
    .filter(F.col("bank_transfer_count") < 37)
    .orderBy("status")
)
