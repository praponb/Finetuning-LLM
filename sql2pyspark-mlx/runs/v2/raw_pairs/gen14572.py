from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .filter(F.col("accounts.customer_id").isNotNull())
    .join(df_customers.filter(~F.col("is_active")).alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("accounts.balance") >= 41000)
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.balance"),
    )
)
