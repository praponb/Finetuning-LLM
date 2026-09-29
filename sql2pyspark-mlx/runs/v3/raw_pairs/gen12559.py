from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.name"))
    .agg(
        F.count(F.col("accounts.id")).alias("account_count"),
        F.sum(F.when(F.col("accounts.status") == "CLOSED", F.col("accounts.balance")).otherwise(0)).alias("closed_balance"),
    )
    .orderBy("id")
)
