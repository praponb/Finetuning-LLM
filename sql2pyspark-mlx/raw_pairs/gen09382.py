from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 10))
    .select("id", "duration_sec")
    .intersect(
        df_web_events
        .filter(F.col("event_type").isin(["click", "search", "page_view"]))
        .select("id", "duration_sec")
    )
)
