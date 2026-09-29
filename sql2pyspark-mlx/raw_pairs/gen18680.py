from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec")
w2 = Window.partitionBy("event_type")

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_type",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.avg("duration_sec").over(w2).alias("event_type_avg_duration_sec"),
    )
    .orderBy("duration_sec")
)
