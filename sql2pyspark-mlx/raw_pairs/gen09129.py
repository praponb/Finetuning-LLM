from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "event_time"))
    .cube("is_new_burst_tier", "session_id", "quarter_start")
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.size(F.collect_set("event_type")).alias("num_event_types_2"),
        F.count("*").alias("event_count"),
        F.grouping("is_new_burst_tier").alias("grouping_level"),
    )
    .orderBy(F.col("num_pages").asc_nulls_last())
)
