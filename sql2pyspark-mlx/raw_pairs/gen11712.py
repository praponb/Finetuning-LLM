from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_type_length", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("duration_sec", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("page")
w4 = Window.partitionBy("session_id").orderBy("event_type_length", "id").rowsBetween(-2, -1)
w5 = Window.orderBy(F.col("event_type_length").desc(), "id")

filtered = (
    df_web_events
    .filter(F.col("event_time") < "2024-01-13")
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.length("event_type").alias("event_type_length"),
    )
)

result = (
    filtered
    .filter(F.col("duration_sec") >= 143)
    .select(
        "id",
        "duration_sec",
        "event_type_length",
        F.avg("event_type_length").over(w).alias("event_type_length_avg_6"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2_prior"),
        F.avg("duration_sec").over(w3).alias("page_avg_duration_sec"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_2_prior_2"),
        F.ntile(4).over(w5).alias("event_type_length_bucket"),
    )
)
