from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("cs.tablet_count"),
    )
    .orderBy(F.col("tablet_count").desc(), "id")
)
