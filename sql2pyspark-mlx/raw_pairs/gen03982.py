from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 12800, "high").when(F.col("c.credit_limit") >= 3100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.max(F.col("t.txn_time")).alias("last_txn_time"),
        F.sum(F.when(F.col("t.merchant") == "Shell", F.col("t.amount")).otherwise(0)).alias("shell_amount"),
    )
)
