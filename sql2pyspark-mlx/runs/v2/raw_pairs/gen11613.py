from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14300, "high").when(F.col("c.credit_limit") >= 6900, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.page")))).alias("page_values"),
    )
)
