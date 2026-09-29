from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
    .filter(F.col("bank_transfer_count") > 1)
)
