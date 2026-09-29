from pyspark.sql import functions as F

min_duration_sec = df_web_events.filter(F.col("page").isin(["/cart", "/search"])).agg(F.min("duration_sec")).first()[0]

result = (
    df_web_events
    .filter(F.col("duration_sec") < min_duration_sec)
    .select("id", "duration_sec", "session_id", "event_time")
    .orderBy(F.col("duration_sec").desc())
)
