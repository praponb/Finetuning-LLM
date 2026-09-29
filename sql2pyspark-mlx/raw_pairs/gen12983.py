from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(F.col("customers.credit_limit").isNull())
    .select(
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
    )
)
