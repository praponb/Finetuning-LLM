from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 11))
    .select("session_id", "page")
    .unionByName(
        df_web_events
        .filter(F.hour("event_time").between(8, 15))
        .select("session_id", "page")
    )
)
