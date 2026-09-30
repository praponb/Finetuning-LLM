from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(
        F.col("customers.country"),
        F.col("customers.segment"),
        F.col("customer_stats.open_count"),
    )
)
