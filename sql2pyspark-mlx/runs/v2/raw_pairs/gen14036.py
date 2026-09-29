from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("t.txn_time") >= "2024-05-08") & F.col("a.account_type").isin(["CREDIT", "LOAN"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
        F.upper(F.col("a.account_type")).alias("account_type_upper"),
    )
    .orderBy("opened_date")
)
