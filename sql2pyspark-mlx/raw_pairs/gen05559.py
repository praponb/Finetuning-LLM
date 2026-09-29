from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

result = (
    valid
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.account_type") != "SAVINGS")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_time",
        "a.is_flagged",
        "a.amount_filled",
        F.col("p2.account_type"),
        F.col("p2.status"),
    )
)
