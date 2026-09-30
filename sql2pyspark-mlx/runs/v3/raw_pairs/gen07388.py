from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") != "London")
    .groupBy(F.col("c.is_active"))
    .agg(F.sort_array(F.collect_set(F.col("s.device"))).alias("device_list"))
)
