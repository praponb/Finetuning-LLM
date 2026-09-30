from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.upper(F.col("a.account_type")).alias("account_type_upper"),
    )
    .orderBy(F.col("amount").desc())
)
