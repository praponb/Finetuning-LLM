from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") != "add_to_cart") & F.col("duration_sec").between(190, 263))
    .select(
        "id",
        "event_time",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.lower("event_type").alias("event_type_lower"),
    )
)
