from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.countDistinct("device").alias("unique_devices"),
        F.min("started_at").alias("min_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.unique_devices") >= 3)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("ct.unique_devices"),
        F.col("ct.min_started_at"),
    )
)
