from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("*", F.length("page").alias("page_length"))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("session_id")
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("page_view_duration_sec") > 0)
)
