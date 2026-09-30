from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type") != "search")
        .select("event_type")
    )
)
