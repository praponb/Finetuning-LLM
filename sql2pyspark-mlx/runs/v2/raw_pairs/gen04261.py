from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.hour("paid_at").alias("paid_hour"),
        F.quarter("paid_at").alias("paid_quarter"),
    )
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 1240)
)
