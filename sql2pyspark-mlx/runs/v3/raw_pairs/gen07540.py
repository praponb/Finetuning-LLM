from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .filter(F.col("customers.country").isin(["US", "TH", "DE"]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("customers.credit_limit"),
        F.col("customers.country"),
        F.col("customers.email"),
    )
    .orderBy("email")
)
