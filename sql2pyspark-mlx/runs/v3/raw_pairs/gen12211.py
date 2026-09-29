from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.email"))
    .agg(
        F.count(F.col("accounts.id")).alias("account_count"),
        F.avg(F.col("accounts.balance")).alias("avg_balance"),
        F.count(F.when(F.col("accounts.status") == "FROZEN", 1)).alias("frozen_count"),
    )
    .filter(F.col("frozen_count") > 4)
)
