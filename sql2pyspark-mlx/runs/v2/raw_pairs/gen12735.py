from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("accounts.account_type") == "CHECKING")
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("transactions.txn_type"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("customers.signup_date"),
    )
    .orderBy(F.col("status").desc())
)
