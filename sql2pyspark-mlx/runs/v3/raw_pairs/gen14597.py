from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_type",
        F.upper("page").alias("page_upper"),
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
    .orderBy(F.col("page_upper").desc())
)
