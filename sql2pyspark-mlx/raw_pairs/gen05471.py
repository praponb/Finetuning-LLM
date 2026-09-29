from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("event_time"), "page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("event_type", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("running_sum_duration_sec", "id")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id")
w6 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("island_id_tier").orderBy(F.col("click_events").desc())

enriched = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.ntile(4).over(w2).alias("duration_sec_bucket"),
    )
)

with_windows = (
    enriched
    .filter(F.col("duration_sec") == 147)
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w3))
    .withColumn("running_avg_duration_sec", F.avg("duration_sec").over(w3))
    .withColumn("running_min_duration_sec", F.min("duration_sec").over(w3))
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "event_type", "event_time", "duration_sec_bucket")
)

gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w5).isNull() | (F.col("event_time") > F.lag("event_time").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec_bucket",
        F.sum("is_new_session").over(w6).alias("session_no"),
    )
)

gaps_2 = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w5).isNull() | (F.col("event_time") > F.lag("event_time").over(w5) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands_web_events = gaps_2.withColumn("island_id", F.sum("is_new_burst").over(w6))

result = (
    islands_web_events
    .groupBy(
        F.when(F.col("island_id") >= 9, "HIGH").otherwise("LOW").alias("island_id_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.max("event_time").alias("last_event_time"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
    )
    .withColumn("click_events_percent_rank", F.percent_rank().over(w7))
)
