from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("weeks_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id").orderBy("weeks_since_event_time", "id")
w4 = Window.partitionBy("page")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-20 20:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("weeks_since_event_time", (F.datediff(F.current_date(), "event_time") / 7))
)

result = (
    base
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("session_id_sum_duration_sec"),
        F.sum("weeks_since_event_time").over(w2).alias("cumulative_weeks_since_event_time"),
        F.lag("weeks_since_event_time", 1).over(w3).alias("prev_weeks_since_event_time"),
        F.max("weeks_since_event_time").over(w4).alias("page_max_weeks_since_event_time"),
        F.sum("duration_sec").over(w5).alias("running_duration_sec"),
    )
)
