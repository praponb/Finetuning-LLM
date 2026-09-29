from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 121) & F.col("event_type").isin(["search", "page_view"]))
    .withColumn("page_lower", F.lower("page"))
)
