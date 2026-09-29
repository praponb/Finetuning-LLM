from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .groupBy("event_type", F.date_trunc("week", "prev_event_time").alias("week_start"))
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.max(F.when(F.col("page") == "/checkout", F.col("duration_sec"))).alias("checkout_max_duration_sec"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("burst_id_per_checkout"),
        F.max(F.when(F.col("event_type") == "search", F.col("burst_id"))).alias("search_max_burst_id"),
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
)
