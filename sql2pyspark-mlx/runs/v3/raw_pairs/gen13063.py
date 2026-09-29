from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([30, 38]))
    .groupBy("event_type", "page")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
    .orderBy(F.col("unique_pages").desc())
)
