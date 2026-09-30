from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "page",
        "event_type",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.initcap(F.trim("page")).alias("page_title"),
    )
)
