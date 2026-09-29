from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.min("duration_sec").over(w).alias("event_type_min_duration_sec"),
    )
)
