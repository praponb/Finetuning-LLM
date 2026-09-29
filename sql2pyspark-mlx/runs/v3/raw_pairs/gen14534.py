from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 346)
    .select("duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") != "purchase")
        .select("duration_sec")
    )
    .distinct()
)
