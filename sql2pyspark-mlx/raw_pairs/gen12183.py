from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("device"))).alias("device_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.segment"),
        F.col("cs.first_started_at"),
        F.col("cs.device_values"),
    )
    .orderBy(F.col("segment").desc())
)
