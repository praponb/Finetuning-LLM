from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("customers.name"),
        F.col("customers.signup_date"),
        F.col("customers.credit_limit"),
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
        F.col("accounts.status"),
    )
    .orderBy("signup_date")
)
