from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.sum("balance").alias("total_balance"))
)

result = (
    df_customers.alias("customers")
    .join(accounts_by_customer.alias("accounts_by_customer"), F.col("customers.id") == F.col("accounts_by_customer.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
        F.col("accounts_by_customer.total_balance"),
    )
    .orderBy("signup_date")
)
