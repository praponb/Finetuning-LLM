from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("burst_id_per_purchase"),
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)
