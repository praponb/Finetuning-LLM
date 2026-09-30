from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("device") != "tablet")
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max("started_at").alias("latest_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("C.device_list"),
        F.col("C.latest_started_at"),
    )
    .orderBy("id")
)
