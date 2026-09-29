from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.credit_limit") >= 6000)
    .select(
        F.col("accounts.status"),
        F.col("accounts.opened_date"),
        F.col("customers.age"),
        F.trim(F.col("accounts.account_type")).alias("clean_account_type"),
    )
)
