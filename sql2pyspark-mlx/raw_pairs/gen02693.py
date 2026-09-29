from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & (F.col("event_type") == "page_view"))
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
        F.round((F.col("duration_sec") * 1.1), 2).alias("duration_sec_adjusted"),
    )
)
