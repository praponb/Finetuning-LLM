from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 37)
    .select(
        "id",
        "duration_sec",
        "event_time",
        "session_id",
        F.when(F.col("event_type").isin(["click", "add_to_cart"]), "Core").otherwise("Other").alias("event_type_group"),
        F.length("page").alias("page_length"),
    )
)
