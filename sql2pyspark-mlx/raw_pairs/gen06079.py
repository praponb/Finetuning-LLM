from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
    )
)
