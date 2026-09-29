from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy(
        F.when(F.col("amount") >= 3500, "high").when(F.col("amount") >= 1100, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)
