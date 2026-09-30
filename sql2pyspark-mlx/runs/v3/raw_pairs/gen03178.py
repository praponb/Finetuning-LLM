from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age").between(40, 45))
    .groupBy(
        F.when(F.col("c.credit_limit") >= 15900, "high").when(F.col("c.credit_limit") >= 6400, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.segment"),
    )
    .agg(
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
    )
    .filter(F.col("avg_amount") > 56)
)
