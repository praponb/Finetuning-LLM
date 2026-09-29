from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.min(F.col("accounts.balance")).alias("min_balance"),
        F.max(F.col("accounts.opened_date")).alias("last_opened_date"),
    )
)
