from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.lower(F.trim(F.col("c.email"))) == "henry@startup.io") & (F.col("a.balance") >= 45000))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.city"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc(), "id")
    .limit(10)
)
