from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.balance_count") >= 9)
    .select(
        F.col("c.age"),
        F.col("c.segment"),
        F.col("cs.balance_count"),
        F.col("cs.closed_count"),
    )
    .orderBy("segment")
)
