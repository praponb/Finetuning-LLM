from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("page").isin(["/home", "/checkout", "/cart"])).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_anti")
    .select(F.col("sessions.id"), F.col("sessions.customer_id"))
)
