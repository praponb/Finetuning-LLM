from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").asc_nulls_last())
w4 = Window.partitionBy("event_type", "session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.cume_dist().over(w3).alias("duration_sec_cume_dist"),
        F.lag("duration_sec", 1).over(w4).alias("prev_duration_sec"),
    )
)
