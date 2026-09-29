from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("t.account_id").isin([14, 26, 30]))
    .groupBy(F.col("a.account_type"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.count(F.when(F.col("t.txn_type") == "FEE", 1)).alias("fee_count"),
    )
    .orderBy("account_type")
)
