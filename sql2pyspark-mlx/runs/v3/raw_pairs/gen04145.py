from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("customer_id")
    .agg(
        F.max("balance").alias("max_balance"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.max_balance") > 22000)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("t.max_balance"),
        F.col("t.total_count"),
    )
)
