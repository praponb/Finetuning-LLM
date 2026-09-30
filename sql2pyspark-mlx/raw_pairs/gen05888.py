from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("device"))).alias("device_values"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.edge_count") >= 5)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("sbc.device_values"),
        F.col("sbc.edge_count"),
    )
    .orderBy("edge_count")
)
