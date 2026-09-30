from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.amount").between(1600, 3200))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.when(F.col("t.txn_type").isin(["CREDIT", "DEBIT"]), "Tier 1").otherwise("Other").alias("txn_type_group"),
    )
)
