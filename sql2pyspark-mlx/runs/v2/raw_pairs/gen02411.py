from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("t.account_id") == 3)
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.txn_type"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("c.city"),
        F.ceil(F.col("c.age")).alias("age_whole"),
    )
    .orderBy(F.col("customer_id").desc())
)
