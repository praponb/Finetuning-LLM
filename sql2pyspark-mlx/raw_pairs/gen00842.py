from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance").between(35000, 55000)).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.txn_type").isin(["CREDIT", "FEE"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
    )
    .orderBy("txn_type")
)
