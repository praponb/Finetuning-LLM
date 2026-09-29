from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 15))
    .cube("merchant", "is_flagged")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .orderBy(F.col("refund_count").desc())
)
