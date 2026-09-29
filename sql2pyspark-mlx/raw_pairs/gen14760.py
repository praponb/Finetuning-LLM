from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc_nulls_last())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.max("duration_sec").over(w).alias("last7_max_duration_sec"),
        F.min("duration_sec").over(w).alias("last7_min_duration_sec"),
        F.count("duration_sec").over(w).alias("last7_count_duration_sec"),
        F.sum("duration_sec").over(w).alias("last7_sum_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)
