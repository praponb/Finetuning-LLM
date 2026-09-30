from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("signup_date").between("2023-05-28", "2024-08-01")).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.status"),
    )
    .orderBy("id")
)
