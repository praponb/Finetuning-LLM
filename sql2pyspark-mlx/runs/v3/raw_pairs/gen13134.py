from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(F.col("customers.city").isin(["Berlin", "Tokyo"]) & (F.col("accounts.customer_id") == 33))
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("customers.segment"),
    )
    .orderBy("account_type")
)
