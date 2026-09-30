from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

result = (
    base
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.account_type").alias("account_type"),
        F.col("p.status").alias("status"),
        F.col("a.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.amount")).alias("min_amount"),
    )
    .orderBy(F.col("account_type").desc())
)
