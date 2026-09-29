from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type") == "FEE").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.status").isin(["CLOSED", "FROZEN"]))
    .select(F.col("a.id"), F.col("a.status"), F.col("a.opened_date"))
    .orderBy("status", "id")
    .limit(5)
)
