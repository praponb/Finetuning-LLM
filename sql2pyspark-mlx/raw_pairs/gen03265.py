from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(F.col("customers.segment").isNotNull())
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
        F.col("customers.email"),
        F.col("customers.credit_limit"),
        F.col("customers.is_active"),
    )
)
