from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy(F.year("event_time"), "session_id").orderBy("event_time", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_last())

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6_prior"),
        F.rank().over(w3).alias("duration_sec_rank"),
    )
)
