from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("page_event_type_key", F.concat_ws(",", "page", "event_type"))
    .withColumn("page_event_type_key_2", F.concat_ws(",", "page", "event_type"))
    .orderBy("session_id")
)
