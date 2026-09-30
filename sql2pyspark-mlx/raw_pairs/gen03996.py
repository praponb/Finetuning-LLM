from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") >= "2023-09-01") & F.col("is_flagged") & F.col("is_flagged"))
)

amount_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1000, "mid").otherwise("low").alias("amount_bucket"),
        "txn_type",
    )
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = amount_bucket_summary
