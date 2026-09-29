from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "search") & F.col("session_id").isin([9, 36, 40]))
    .select("id", "event_type", "page")
    .orderBy(F.col("page").desc())
)
