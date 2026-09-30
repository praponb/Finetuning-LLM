from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("event_type_sum_duration_sec"),
    )
)
