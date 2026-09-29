from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select(
        "id",
        "event_time",
        "duration_sec",
        "event_type",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.dense_rank().over(w2).alias("event_time_dense_rank"),
    )
)
