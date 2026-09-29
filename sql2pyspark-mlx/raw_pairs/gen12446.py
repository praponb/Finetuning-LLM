from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull())
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("row_count") >= 4)
    .select("event_quarter", "row_count", "last_event_time")
    .orderBy(F.col("row_count").desc())
)
