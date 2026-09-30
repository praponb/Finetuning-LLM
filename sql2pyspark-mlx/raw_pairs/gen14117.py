from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("status").isNull())
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.closed_count") >= 53)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.segment"),
        F.col("t.closed_count"),
        F.col("t.avg_balance"),
    )
    .orderBy("city", "segment")
)
