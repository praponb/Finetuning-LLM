from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .groupBy(F.col("customers.country"))
    .agg(
        F.sum(F.when(F.col("transactions.txn_type") == "FEE", F.col("transactions.amount")).otherwise(0)).alias("fee_amount"),
        F.sum(F.when(F.col("transactions.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .orderBy(F.col("country").desc(), "fee_amount")
)
