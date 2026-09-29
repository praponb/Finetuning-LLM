from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time_month").desc_nulls_last())
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("session_id")
w4 = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")
w5 = Window.partitionBy("session_id").orderBy(F.col("event_time_month").desc(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= "2023-12-22"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.month("event_time").alias("event_time_month"),
    )
)

result = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("event_time_month_cume_dist"),
        F.avg("event_time_month").over(w2).alias("event_time_month_avg_5"),
        F.stddev_samp("duration_sec").over(w3).alias("session_id_stddev_samp_duration_sec"),
        F.lead("event_time_month", 1).over(w4).alias("next_event_time_month"),
        F.ntile(4).over(w5).alias("event_time_month_bucket"),
    )
)
