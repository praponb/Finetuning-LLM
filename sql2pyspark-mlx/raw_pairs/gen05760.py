from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_last())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("duration_sec").over(w_recent).alias("duration_sec_sum_7d"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)
