from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.merchant").isin(["Grab", "Shell"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
    )
    .orderBy(F.col("id").desc())
)
