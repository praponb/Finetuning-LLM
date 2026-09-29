from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.avg_balance") >= 29)
    .select(F.col("c.segment"), F.col("C.avg_balance"))
    .orderBy(F.col("avg_balance").desc(), "segment")
)
