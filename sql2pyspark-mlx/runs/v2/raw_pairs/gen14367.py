from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("a.balance") >= 64000, "high").when(F.col("a.balance") >= 9000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count(F.when(F.col("t.txn_type") == "FEE", 1)).alias("fee_count"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
)
