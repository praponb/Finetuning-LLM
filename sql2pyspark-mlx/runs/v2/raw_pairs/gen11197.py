from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page", "event_type", "duration_sec")
    .orderBy(F.col("duration_sec").desc(), "page")
)
