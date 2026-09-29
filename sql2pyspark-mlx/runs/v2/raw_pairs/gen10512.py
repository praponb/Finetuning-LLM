from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("start_date").alias("min_start_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.country"), F.col("cs.status_values"), F.col("cs.min_start_date"))
)
