from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(F.col("accounts.status") == "OPEN")
    .select(
        F.col("transactions.id"),
        F.col("transactions.is_flagged"),
        F.col("accounts.opened_date"),
        F.col("customers.signup_date"),
        F.upper(F.col("transactions.merchant")).alias("merchant_upper"),
    )
)
