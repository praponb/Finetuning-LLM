from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .filter(F.col("web_events.page").isin(["/search", "/home", "/checkout"]))
    .groupBy(F.col("sessions.browser"))
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 3)
)
