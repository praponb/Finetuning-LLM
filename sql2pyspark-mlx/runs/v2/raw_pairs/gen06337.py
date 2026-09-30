from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(250, 268) & (F.col("page") != "/product"))
    .select(
        "id",
        "event_type",
        "event_time",
        F.substring("page", 1, 3).alias("page_prefix"),
    )
)
