from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & F.col("is_flagged"))
    .groupBy("txn_type", "merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("unique_accounts") >= 5)
    .drop("unique_accounts")
    .orderBy("pct_grab", "txn_type")
)
