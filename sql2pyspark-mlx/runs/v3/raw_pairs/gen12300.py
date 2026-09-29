from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("page") == "/product").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.started_at"))
    .orderBy(F.col("id").desc())
)
