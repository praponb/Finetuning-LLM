from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country").isin(["SG", "TH"]))
    .groupBy(F.col("a.account_type"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .orderBy(F.col("account_type").desc())
)
