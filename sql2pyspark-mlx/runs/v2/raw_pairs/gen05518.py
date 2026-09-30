from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser") == "Chrome").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.duration_sec").between(188, 383))
    .select(F.col("we.id"), F.col("we.page"))
)
