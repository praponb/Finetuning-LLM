from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("accounts.balance") < 77000)
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("customers.is_active"),
        F.col("customers.name"),
        F.col("customers.country"),
    )
    .orderBy("name")
)
