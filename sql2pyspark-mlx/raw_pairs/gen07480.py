from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.started_at") < "2024-12-01")
    .groupBy(F.col("c.city"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
        F.sort_array(F.collect_set(F.col("s.device"))).alias("device_list"),
    )
)
