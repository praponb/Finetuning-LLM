from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") | F.hour("event_time").between(11, 16))
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        F.round((F.col("duration_sec") * 1.15), 2).alias("adjusted_duration_sec"),
        F.initcap(F.trim("page")).alias("page_title"),
    )
)
