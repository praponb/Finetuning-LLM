from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.segment"), F.col("customers.country"))
    .agg(
        F.sum(F.when(F.col("accounts.status") == "FROZEN", F.col("accounts.balance")).otherwise(0)).alias("frozen_balance"),
    )
    .orderBy("country")
)
