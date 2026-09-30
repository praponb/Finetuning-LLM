from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.sort_array(F.collect_set(F.col("s.device"))).alias("device_list"))
    .orderBy(F.col("segment").desc())
    .limit(50)
)
