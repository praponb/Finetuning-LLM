from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("customer_id") == 5)
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("browser") == "Chrome", 1).otherwise(0)).alias("chrome_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("device"))).alias("device_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.chrome_count") > 60)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.col("sbc.chrome_count"),
        F.col("sbc.device_values"),
    )
)
