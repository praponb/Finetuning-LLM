from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.is_active"),
        F.col("c.name"),
        F.col("cs.avg_balance"),
        F.col("cs.balance_range"),
    )
)
