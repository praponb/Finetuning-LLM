from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-01-01") & (F.col("txn_type") == "REFUND") & (F.col("txn_type") == "FEE"))
    .groupBy(
        "is_flagged",
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
    )
    .filter(F.col("pct_debit") > 54)
    .orderBy("amount_bucket", "is_flagged")
    .limit(5)
)
