from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.city"), F.col("c.name"), F.col("t.closed_count"))
    .orderBy("name")
)
