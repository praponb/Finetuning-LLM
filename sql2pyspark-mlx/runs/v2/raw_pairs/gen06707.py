from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
    .orderBy(F.col("order_id").desc())
)
