from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("signup_date") <= "2024-12-16").alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
    )
    .orderBy("opened_date", F.col("balance").desc())
)
