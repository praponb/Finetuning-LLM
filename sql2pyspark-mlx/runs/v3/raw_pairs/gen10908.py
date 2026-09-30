from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 15))
    .groupBy("status", "method")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
    )
    .orderBy("status")
)
