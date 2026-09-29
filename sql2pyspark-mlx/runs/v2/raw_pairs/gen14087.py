from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("merchant").isNull())
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.credit_count") > 7)
    .select(F.col("a.status"), F.col("t.credit_count"))
    .orderBy(F.col("credit_count").desc())
)
