from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.name"), F.col("cs.balance_range"))
    .orderBy("id", F.col("balance_range").desc())
)
