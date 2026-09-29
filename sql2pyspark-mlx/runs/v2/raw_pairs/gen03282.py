from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "tablet", 1).otherwise(0)).alias("tablet_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.tablet_count") >= 41)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("cs.tablet_count"),
    )
    .orderBy("credit_limit")
)
