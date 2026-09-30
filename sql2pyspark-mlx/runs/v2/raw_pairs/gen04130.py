from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") > 198).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.browser"))
    .orderBy(F.col("browser").desc(), F.col("customer_id").desc())
)
