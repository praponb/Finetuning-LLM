from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2023-06-14 04:00:00")))
    .select("*", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        F.col("p.opened_date"),
        F.col("p.status"),
    )
)

cleaned = (
    enriched
    .filter((F.col("status").eqNullSafe(F.lit("FROZEN"))) & F.col("account_id").isin([25, 38]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "opened_date",
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.status"),
        F.col("b.balance"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "txn_type",
        "opened_date",
        "is_frozen",
        "account_opened_date",
        "status",
        "balance",
    )
    .orderBy(F.col("balance").desc())
)
