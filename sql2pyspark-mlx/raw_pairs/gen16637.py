from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.segment") == "Consumer")
    .groupBy(
        F.when(F.col("s.country").isin(["JP", "UK"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.max(F.col("we.duration_sec")).alias("max_duration_sec"))
    .orderBy("country_group")
)
