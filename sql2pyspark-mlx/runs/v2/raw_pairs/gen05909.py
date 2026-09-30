from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "page_view")
    .select("event_type", "page")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") >= "2024-03-01")
        .select("event_type", "page")
    )
    .orderBy("page")
)
