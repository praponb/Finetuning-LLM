from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "is_flagged",
        "merchant",
        "txn_type",
        F.when(F.col("merchant").isin(["Starbucks", "Grab"]), "Tier 1").otherwise("Other").alias("merchant_group"),
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
    )
)
