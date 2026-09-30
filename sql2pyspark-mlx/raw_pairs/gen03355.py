from pyspark.sql import functions as F

src = df_web_events.filter(F.col("event_type") == "search")

page_summary = (
    src
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
)

result = page_summary.select("page", "page_view_duration_sec")
