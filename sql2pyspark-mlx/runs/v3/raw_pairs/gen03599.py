from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select("event_type")
    .unionByName(
        df_web_events
        .filter(F.hour("event_time").between(8, 12))
        .select("event_type")
    )
    .orderBy(F.col("event_type").desc())
)
