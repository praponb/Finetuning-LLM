from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("days_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type", "page").orderBy(F.col("days_since_event_time").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy("days_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") < "2025-10-27")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.when(F.col("duration_sec") >= 439, "High").when(F.col("duration_sec") >= 106, "Medium").otherwise("Low").alias("duration_sec_tier"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

metrics = (
    cleaned
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("running_days_since_event_time", F.sum("days_since_event_time").over(w3))
)

joined = (
    metrics
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.device"), F.col("d.browser"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.device").alias("session_device"),
        F.col("b.country"),
        F.col("b.browser").alias("session_browser"),
    )
)

result = (
    with_dims
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.percentile_approx("days_since_event_time", [0.25, 0.5, 0.75]).alias("days_since_event_time_quartiles"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("days_since_event_time"))).alias("page_view_avg_days_since_event_time"),
        F.count("*").alias("event_count"),
        F.max("days_since_event_time").alias("max_days_since_event_time"),
        F.size(F.collect_set("session_browser")).alias("num_session_browsers"),
    )
)
