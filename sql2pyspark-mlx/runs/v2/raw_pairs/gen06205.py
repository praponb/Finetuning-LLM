from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.col("customers.city"),
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("web_events.page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
        F.count(F.when(F.col("web_events.event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .orderBy(F.col("pct_product").desc())
)
