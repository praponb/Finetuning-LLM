from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & (F.col("page") == "/home"))
    .select(
        "id",
        "event_time",
        "event_type",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
    .orderBy("event_time_label", F.col("event_type").desc())
)
