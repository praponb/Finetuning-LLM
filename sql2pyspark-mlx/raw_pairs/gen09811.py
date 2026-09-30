from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .select(
        F.col("customers.email"),
        F.col("customers.credit_limit"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
    )
)
