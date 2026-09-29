from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.year(F.col("c.signup_date")).alias("signup_year"),
        F.when(F.col("c.city").isin(["Chiang Mai", "London"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
)
