from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("event_type")
    .agg(
        ((F.unix_timestamp(F.max("prev_event_time")) - F.unix_timestamp(F.min("prev_event_time"))) / 60).alias("duration_min"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.percentile_approx("session_id_2", 0.9).alias("session_id_2_p90"),
        F.min("event_time").alias("first_event_time"),
        F.percentile_approx("session_id_2", 0.95).alias("session_id_2_p95"),
    )
)
