from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(F.col("accounts.balance") < 78000)
    .select(
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("customers.city"),
        F.col("customers.email"),
    )
)
