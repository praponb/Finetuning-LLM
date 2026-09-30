from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(~F.col("accounts.account_type").isin(["SAVINGS", "CREDIT"]))
    .select(
        F.col("accounts.balance"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("customers.email"),
        F.col("customers.name"),
    )
)
