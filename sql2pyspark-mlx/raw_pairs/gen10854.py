from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "event_type", "page")
    .orderBy("page", "id")
    .limit(50)
)
