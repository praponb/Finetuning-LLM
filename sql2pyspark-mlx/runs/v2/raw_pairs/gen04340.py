from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.browser").isin(["Firefox", "Chrome", "Edge"]) & (F.col("s.device") == "desktop"))
    .select(F.col("we.page"), F.col("s.device"), F.col("s.customer_id"))
    .orderBy(F.col("device").desc())
)
