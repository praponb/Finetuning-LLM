from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("page") == "/product").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.device").isin(["tablet", "mobile"]))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("s.customer_id"))
)
