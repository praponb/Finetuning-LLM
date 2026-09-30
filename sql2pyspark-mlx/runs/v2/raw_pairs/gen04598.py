from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
        F.col("customers.is_active"),
        F.col("customers.name"),
        F.col("customers.signup_date"),
    )
    .orderBy(F.col("id").desc(), F.col("account_type").desc())
)
