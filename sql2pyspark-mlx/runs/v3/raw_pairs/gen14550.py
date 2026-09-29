from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
        F.col("customers.credit_limit"),
        F.col("customers.email"),
    )
)
