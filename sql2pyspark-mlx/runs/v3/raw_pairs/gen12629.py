from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.total_amount") > 14000)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("t.fee_amount"),
        F.col("t.total_amount"),
    )
    .orderBy("customer_id")
)
