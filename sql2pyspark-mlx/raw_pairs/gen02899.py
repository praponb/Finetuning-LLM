from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Chrome"))))
    .select("id", "customer_id", "device", F.lower("device").alias("device_lower"))
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") > 412), "left_anti")
)

eligible_sessions = (
    eligible
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") != "search"), "left_semi")
)

eligible_sessions_2 = (
    eligible_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < "2023-04-18"), "left_anti")
)

kept = (
    eligible_sessions_2
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type").isin(["add_to_cart", "search", "purchase"])), "left_semi")
)

result = (
    kept
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "device_lower", F.col("min_order_discount"))
)
