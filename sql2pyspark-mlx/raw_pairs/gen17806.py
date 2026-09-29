from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.avg_balance") > 25000)
    .select(F.col("customers.name"), F.col("t.avg_balance"), F.col("t.total_balance"))
    .orderBy(F.col("total_balance").desc())
)
