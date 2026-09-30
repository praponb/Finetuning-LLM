from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 16, 22, 24]))
    .select("event_type")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([13, 26, 37]))
        .select("event_type")
    )
    .distinct()
)
