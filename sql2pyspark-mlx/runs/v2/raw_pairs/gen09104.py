from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("c.credit_limit"),
        F.when(F.col("t.is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("balance")
)
