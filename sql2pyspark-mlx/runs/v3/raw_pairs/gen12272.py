from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("started_at") >= "2025-10-28")
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("device")).alias("device_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.col("cs.device_list"),
    )
)
