from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("started_at").alias("min_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.segment"), F.col("cs.country_list"), F.col("cs.min_started_at"))
)
