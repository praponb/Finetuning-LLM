from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.count(F.when(F.col("a.status") == "FROZEN", 1)).alias("frozen_count"),
        F.min(F.col("a.balance")).alias("min_balance"),
    )
)
