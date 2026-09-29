from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "session_id",
        "duration_sec",
        F.max("duration_sec").over(w).alias("event_type_max_duration_sec"),
    )
    .orderBy(F.col("event_type_max_duration_sec").desc())
)
