from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.name").like("%Pim%"))
    .groupBy(F.col("c.segment"), F.col("c.city"))
    .agg(
        F.min(F.col("s.started_at")).alias("min_started_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
    )
)
