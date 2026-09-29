from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 13))
    .select("id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_type").isin(["search", "purchase"]))
        .select("id", "duration_sec")
    )
    .distinct()
    .orderBy("duration_sec")
)
