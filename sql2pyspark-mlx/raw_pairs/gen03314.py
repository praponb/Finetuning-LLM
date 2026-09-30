from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("duration_sec").between(307, 386))
    .select("*", F.when(F.col("page") == "/home", 1).otherwise(0).alias("is_home"))
)

prev_events = base.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.min("session_no").alias("min_session_no"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
    )
    .orderBy(F.col("search_events").desc_nulls_last())
)
