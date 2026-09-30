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
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.desktop_count") > 51)
    .select(F.col("c.name"), F.col("cs.desktop_count"))
)
