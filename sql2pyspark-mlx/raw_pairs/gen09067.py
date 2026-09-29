from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
        "merchant",
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
