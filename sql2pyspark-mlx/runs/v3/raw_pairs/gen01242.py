from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser") == "Chrome").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.page").isin(["/cart", "/checkout", "/home"]))
    .select(F.col("we.id"), F.col("we.session_id"), F.col("we.page"))
)
