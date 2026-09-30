from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("accounts.customer_id").isNull())
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("customers.signup_date"),
        F.col("customers.credit_limit"),
        F.col("customers.is_active"),
    )
)
