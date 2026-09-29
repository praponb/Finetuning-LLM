from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_time", "id")
w4 = Window.orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("page")
w6 = Window.partitionBy("country").orderBy("event_time", "id")
w7 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.country"))
)

metrics = (
    with_dims
    .filter(F.col("page") == "/home")
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("event_type_avg_duration_sec"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_2"),
        F.avg("duration_sec").over(w5).alias("page_avg_duration_sec"),
    )
)

with_gaps = (
    metrics
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "event_time",
        "country",
        "event_type_avg_duration_sec",
        "running_duration_sec",
        "next_duration_sec",
        F.sum("is_new_burst").over(w7).alias("session_no"),
    )
)

gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("is_new_burst").over(w7))

result = (
    sessions_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.max(F.when(F.col("country") == "JP", F.col("running_duration_sec"))).alias("jp_max_running_duration_sec"),
    )
)
