from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("device"))).alias("device_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("sbc.last_started_at"),
        F.col("sbc.device_values"),
    )
)
