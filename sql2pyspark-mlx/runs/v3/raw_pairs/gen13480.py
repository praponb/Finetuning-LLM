from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("txn_type").isin(["REFUND", "FEE", "CREDIT"])).alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_anti")
    .filter(F.col("accounts.account_type").isin(["SAVINGS", "CREDIT", "LOAN"]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
    )
)
