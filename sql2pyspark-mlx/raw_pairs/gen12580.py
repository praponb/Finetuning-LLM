from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.is_active"), F.col("ct.balance_count"), F.col("ct.stddev_balance"))
)
