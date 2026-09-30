from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.customer_id").isin([2, 17, 30, 34]))
    .select(F.col("s.id"), F.col("s.browser"))
)
