from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 16, 17]) & F.col("account_id").isin([6, 16, 34, 38]))
    .select(
        "txn_time",
        F.when(F.col("merchant").isin(["Amazon", "Lazada"]), "Tier 1").otherwise("Other").alias("merchant_group"),
        F.when(F.col("merchant") == "Lazada", 1).otherwise(0).alias("is_lazada"),
    )
)
