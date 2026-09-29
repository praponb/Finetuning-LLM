from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 16))
    .select("id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_type").isin(["add_to_cart", "page_view"]))
        .select("id", "duration_sec")
    )
)
