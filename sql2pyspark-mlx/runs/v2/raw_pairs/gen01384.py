from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "purchase")
    .select("event_type")
    .unionByName(
        df_web_events
        .filter(F.to_date("event_time") < "2023-05-06")
        .select("event_type")
    )
    .orderBy(F.col("event_type").desc())
)
