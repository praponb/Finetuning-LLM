from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .select("id", "page")
    .unionByName(
        df_web_events
        .filter(F.hour("event_time").between(8, 14))
        .select("id", "page")
    )
    .orderBy("id")
)
