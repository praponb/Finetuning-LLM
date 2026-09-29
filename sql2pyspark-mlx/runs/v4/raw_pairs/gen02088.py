from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
        F.col("customers.is_active"),
        F.col("customers.segment"),
        F.col("customers.signup_date"),
    )
    .orderBy(F.col("balance").desc())
)
