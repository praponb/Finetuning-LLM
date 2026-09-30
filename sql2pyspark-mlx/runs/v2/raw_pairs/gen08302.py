from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "outer")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("transactions.amount") > 1800) & (F.col("customers.city") == "Singapore"))
    .select(
        F.col("transactions.merchant"),
        F.col("accounts.opened_date"),
        F.col("customers.segment"),
    )
    .orderBy("merchant", "opened_date")
)
