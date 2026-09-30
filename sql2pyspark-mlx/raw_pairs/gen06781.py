from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 14, 34]))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
    .withColumn("page_filled", F.coalesce("page", F.lit("unassigned")))
    .orderBy("event_time")
)
