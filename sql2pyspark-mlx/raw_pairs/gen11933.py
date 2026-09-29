from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.month(F.col("a.opened_date")).isin([4, 5, 8]))
    .select(
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("c.country"),
    )
    .orderBy("txn_type")
)
