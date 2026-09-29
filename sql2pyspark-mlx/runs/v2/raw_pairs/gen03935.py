from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "page",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)
