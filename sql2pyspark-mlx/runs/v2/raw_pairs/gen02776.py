from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(~F.col("event_type").isin(["purchase", "add_to_cart", "click"]))
)

summary = (
    valid_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    summary
    .filter(F.col("avg_duration_sec") > 43)
    .select("event_month", "avg_duration_sec")
    .orderBy(F.col("event_month").desc(), F.col("avg_duration_sec").desc())
    .limit(10)
)
