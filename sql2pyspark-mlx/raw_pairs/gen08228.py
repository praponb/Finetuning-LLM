from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.partitionBy(F.month("event_time"), "event_type").orderBy(F.col("duration_sec").asc_nulls_last())
w3 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id")
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("event_type")

metrics = (
    df_web_events
    .select(
        "*",
        F.max("duration_sec").over(w).alias("session_id_max_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
        F.ntile(4).over(w4).alias("duration_sec_bucket_2"),
        F.sum("duration_sec").over(w5).alias("event_type_sum_duration_sec"),
    )
)

result = (
    metrics
    .cube(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("event_time").alias("last_event_time_2"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
