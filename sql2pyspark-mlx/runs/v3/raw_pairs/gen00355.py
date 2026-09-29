from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 34]))
    .select(
        "id",
        "event_time",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.concat("event_type", F.lit("/"), "page").alias("event_type_page"),
    )
)
