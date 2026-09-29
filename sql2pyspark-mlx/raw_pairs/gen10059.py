from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.max("event_time").alias("last_seen"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("session_id_2")).otherwise(0)).alias("purchase_session_id_2"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_rows"),
    )
)
