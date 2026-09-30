from pyspark.sql import functions as F

enriched = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.account_type").isin(["CHECKING", "SAVINGS"]))
    .select(
        "src.*",
        F.col("b.opened_date"),
        F.col("b.balance"),
        F.col("b.account_type"),
    )
)

result = (
    enriched
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "opened_date",
        "account_type",
    )
    .orderBy(F.col("opened_date").desc(), F.col("txn_type").desc())
)
