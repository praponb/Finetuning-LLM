from pyspark.sql import functions as F

src = df_web_events.filter(F.col("event_time") < "2025-04-01")

page_summary = (
    src
    .groupBy("page")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = page_summary.orderBy(F.col("event_type_values").desc())
