from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .groupBy(
        F.when(F.col("merchant").isin(["Grab", "7-Eleven"]), "tier_1").otherwise("other").alias("merchant_group"),
        "is_flagged",
    )
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("unique_accounts") > 4)
)
