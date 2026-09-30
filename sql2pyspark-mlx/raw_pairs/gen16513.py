from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.to_date("event_time"), "page").orderBy(F.col("duration_sec").desc_nulls_last())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        F.max("duration_sec").over(w).alias("last4_max_duration_sec"),
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.sum("duration_sec").over(w).alias("last4_sum_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)
