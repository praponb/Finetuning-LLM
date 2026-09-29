from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type") == "FEE").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED"]))
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.balance"))
    .orderBy(F.col("balance").desc())
)
