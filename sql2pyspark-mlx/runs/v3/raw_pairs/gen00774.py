from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("customers.signup_date"),
    )
)
