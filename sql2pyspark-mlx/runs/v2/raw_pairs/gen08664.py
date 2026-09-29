from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.city") == "Singapore")
    .groupBy(F.col("customers.segment"))
    .agg(
        F.sum(F.when(F.col("accounts.status") == "FROZEN", F.col("accounts.balance")).otherwise(0)).alias("frozen_balance"),
    )
    .filter(F.col("frozen_balance") >= 57)
)
