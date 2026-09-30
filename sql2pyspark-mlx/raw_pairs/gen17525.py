from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox", "Safari"]))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("browser"))).alias("browser_values"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("sbc.browser_values"),
        F.col("sbc.device_list"),
    )
    .orderBy(F.col("city").desc(), F.col("id").desc())
)
