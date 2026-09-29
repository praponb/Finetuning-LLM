from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("a.status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
        (F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"),
    )
)
