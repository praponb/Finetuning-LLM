from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select("id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("event_time") < "2023-06-01")
        .select("id", "duration_sec")
    )
    .distinct()
)
