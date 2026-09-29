from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("c.credit_limit") < 8500) & (F.col("t.merchant") == "Grab"))
    .select(
        F.col("t.txn_type"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
