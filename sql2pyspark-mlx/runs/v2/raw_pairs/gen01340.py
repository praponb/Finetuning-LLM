from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("accounts.opened_date") < "2024-11-01") & (F.col("accounts.customer_id") == 13))
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("customers.signup_date"),
        F.abs(F.col("accounts.balance")).alias("abs_balance"),
    )
)
