from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("page") == "/checkout") & F.col("duration_sec").between(199, 222))
)

event_quarter_summary = (
    recent_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    event_quarter_summary
    .filter(F.col("duration_sec_range") < 51)
    .select("event_quarter", "duration_sec_range", "avg_duration_sec")
)
