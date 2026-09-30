from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.browser") == "Safari")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.customer_id"),
    )
    .orderBy(F.col("browser").desc(), F.col("customer_id").desc(), "id")
    .limit(5)
)
