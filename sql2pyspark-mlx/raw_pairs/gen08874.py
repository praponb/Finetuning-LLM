from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment").isin(["SMB", "Corporate"]))
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("customers.country"),
    )
    .orderBy("status")
)
