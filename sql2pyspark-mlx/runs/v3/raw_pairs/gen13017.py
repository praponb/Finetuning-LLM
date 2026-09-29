from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("s.browser") == "Edge", 1).otherwise(0)).alias("edge_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.device")))).alias("device_values"),
    )
)
