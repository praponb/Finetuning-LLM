from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.countDistinct("page").alias("distinct_pages"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("page").alias("distinct_pages_2"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
    .orderBy(F.col("distinct_pages_2").asc_nulls_last())
)
