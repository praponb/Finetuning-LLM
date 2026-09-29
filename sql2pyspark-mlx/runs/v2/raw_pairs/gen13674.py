from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["card", "cash"]))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
    .orderBy("status")
)
