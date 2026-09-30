from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["purchase", "page_view"]))
    .select("session_id", "page")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") < 138)
        .select("session_id", "page")
    )
    .distinct()
    .orderBy("session_id")
)
