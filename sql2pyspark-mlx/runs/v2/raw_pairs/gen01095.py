from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("page") != "/home") & (F.to_date("event_time") >= "2024-09-01") & F.col("session_id").isin([15, 23, 35, 40]))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 2))
)

page_summary = (
    base
    .groupBy("page")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
)

result = (
    page_summary
    .filter(F.col("unique_pages") > 10)
    .select("page", "unique_pages")
)
