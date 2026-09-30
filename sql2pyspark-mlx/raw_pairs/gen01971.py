from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("WEEK", "prev_event_time"))
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .rollup("week_start", "session_no_tier")
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("session_no_tier")
)
