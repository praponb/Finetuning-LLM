from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-04-01")
    .select("page", "session_id", "event_time", F.trim("page").alias("clean_page"))
    .orderBy(F.col("session_id").desc(), "page")
)
