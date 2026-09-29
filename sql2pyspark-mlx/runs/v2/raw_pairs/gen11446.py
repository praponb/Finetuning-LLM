from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.segment"), F.col("C.desktop_count"))
)
