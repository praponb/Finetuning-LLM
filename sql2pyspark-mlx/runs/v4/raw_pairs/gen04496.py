from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("days_since_event_time", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w3 = Window.partitionBy("event_type", "session_id").orderBy("days_since_event_time_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id", "event_type").orderBy("days_since_event_time", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-06").cast("date"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("days_since_event_time_2", F.datediff(F.current_date(), "event_time"))
)

result = (
    base
    .select(
        "*",
        F.lag("days_since_event_time_2", 1, 0).over(w).alias("prev_days_since_event_time_2"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.avg("duration_sec").over(w_hist).alias("duration_sec_avg_2"),
        F.sum("days_since_event_time_2").over(w5).alias("cumulative_days_since_event_time_2"),
    )
)
