from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.initcap(F.trim(F.col("a.account_type"))).alias("account_type_title"),
    )
    .orderBy(F.col("balance").desc(), F.col("account_type_title").desc())
)
