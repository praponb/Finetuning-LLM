from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.started_at") >= "2024-08-11") & ~F.col("we.page").isin(["/cart", "/checkout"]))
    .select(F.col("s.started_at"), F.col("we.session_id"))
    .orderBy("started_at")
)
