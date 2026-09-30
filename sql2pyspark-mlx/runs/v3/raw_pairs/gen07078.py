from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 3300) & F.col("is_flagged") & (F.col("txn_time") >= "2025-08-01"))
    .rollup(
        "is_flagged",
        F.when(F.col("amount") >= 3000, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("amount").alias("max_amount"),
    )
)
