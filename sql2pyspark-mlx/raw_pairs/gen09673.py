from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .filter(F.col("sbc.total_count") >= 4)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("sbc.total_count"),
        F.col("sbc.country_values"),
    )
)
