from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
        F.col("c.credit_limit"),
    )
    .orderBy("opened_date", "id")
    .limit(10)
)
