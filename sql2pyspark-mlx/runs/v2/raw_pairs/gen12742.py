from pyspark.sql import functions as F

src = (
    df_web_events
    .filter((F.col("event_type") != "add_to_cart") | F.col("duration_sec").between(202, 400))
)

page_summary = (
    src
    .groupBy("page", "event_type")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    page_summary
    .select("page", "event_type", "duration_sec_count")
    .orderBy("duration_sec_count")
)
