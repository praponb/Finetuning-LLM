from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
    .orderBy(F.col("max_amount").desc(), "account_id")
)
