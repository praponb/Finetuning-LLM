from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.lower(F.trim("email")) == "frank@company.co").select("id")

result = (
    df_accounts.alias("accounts")
    .join(excluded_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("accounts.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
    )
)
