from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(~F.col("t.merchant").isin(["Shell", "Lazada"]) & (F.col("a.account_type") == "SAVINGS"))
    .select(
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
    )
    .orderBy(F.col("balance").desc())
)
