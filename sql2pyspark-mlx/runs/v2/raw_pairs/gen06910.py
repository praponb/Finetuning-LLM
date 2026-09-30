from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("cs.uk_count"),
        F.col("cs.tablet_count"),
    )
)
