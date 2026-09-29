from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.signup_date") <= "2023-12-01")
    .select(
        F.col("customers.name"),
        F.col("customers.country"),
        F.col("customers.city"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
    )
    .orderBy("country")
)
