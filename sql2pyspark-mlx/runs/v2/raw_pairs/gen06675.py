from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "page_view") & (F.col("event_type") == "click"))
    .select("id", "event_time", F.initcap(F.trim("page")).alias("page_title"))
)
