from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("txn_type") == "DEBIT") & F.col("is_flagged") & F.col("is_flagged")))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
    .orderBy(F.col("amazon_count").desc(), F.col("pct_refund").desc())
)
