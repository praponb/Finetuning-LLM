from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.sum(F.when(F.col("accounts.status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
    .orderBy(F.col("frozen_count").desc())
)
