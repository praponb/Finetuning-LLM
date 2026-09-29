from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("device"))).alias("device_values"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.segment"), F.col("cs.device_values"), F.col("cs.country_list"))
    .orderBy(F.col("segment").desc(), "device_values")
)
