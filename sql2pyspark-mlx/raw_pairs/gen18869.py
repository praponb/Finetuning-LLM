from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("order_id").isNotNull())

agg = (
    valid_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
)

result = (
    agg
    .select("status", "bank_transfer_count", "failed_count")
    .orderBy("status")
)
