from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "OPEN"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.min("balance").alias("min_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.open_count") >= 45)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("cs.open_count"),
        F.col("cs.min_balance"),
    )
    .orderBy("id")
)
