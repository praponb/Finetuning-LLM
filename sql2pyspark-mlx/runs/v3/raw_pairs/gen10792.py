from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.hour("event_time").between(12, 15) & (F.col("duration_sec") < 205) & F.col("page").isin(["/product", "/checkout", "/search"]))
)

event_type_summary = (
    src
    .groupBy("event_type")
    .agg(F.count("*").alias("cnt"))
)

result = (
    event_type_summary
    .filter(F.col("cnt") > 8)
    .orderBy("event_type")
)
