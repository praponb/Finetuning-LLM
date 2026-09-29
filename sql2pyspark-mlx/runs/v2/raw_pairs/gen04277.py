from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 288)
    .select("id", "event_time", "page")
    .orderBy(F.col("page").desc(), "id")
)
