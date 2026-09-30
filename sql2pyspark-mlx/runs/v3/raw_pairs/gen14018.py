from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("we.event_type") != "search")
    .groupBy(
        F.when(F.col("s.browser").isin(["Chrome", "Safari"]), "group_a").otherwise("other").alias("browser_group"),
    )
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
    )
    .orderBy("browser_group")
    .limit(50)
)
