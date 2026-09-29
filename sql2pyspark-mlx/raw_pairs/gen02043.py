from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("session_no") >= 6, "LARGE").otherwise("SMALL").alias("session_no_tier"),
        F.date_trunc("week", "event_time").alias("week_start"),
    )
    .agg(
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
    )
    .orderBy("duration_sec_p50")
)
