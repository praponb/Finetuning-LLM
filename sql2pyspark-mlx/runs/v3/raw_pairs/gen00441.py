from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("page") == "/search").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.device") == "tablet")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.customer_id"))
    .orderBy("customer_id")
)
