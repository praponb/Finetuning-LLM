from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.country") == "UK")
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.when(F.col("s.country").isin(["DE", "JP"]), "Core").otherwise("Other").alias("country_group"),
    )
)
