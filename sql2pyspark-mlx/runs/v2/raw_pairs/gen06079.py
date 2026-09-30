from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("signup_date").between("2024-04-01", "2024-09-23")).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("accounts.status") != "CLOSED")
    .select(F.col("accounts.id"), F.col("accounts.opened_date"))
)
