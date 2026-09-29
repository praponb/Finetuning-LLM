from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)
