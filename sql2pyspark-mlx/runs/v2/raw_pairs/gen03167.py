from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/cart"]) | (F.to_date("event_time") >= "2024-03-02"))
)

summary = (
    recent_web_events
    .groupBy("page")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)

result = (
    summary
    .filter(F.col("unique_event_types") < 7)
    .select("page", "unique_event_types")
    .orderBy(F.col("page").desc())
    .limit(5)
)
