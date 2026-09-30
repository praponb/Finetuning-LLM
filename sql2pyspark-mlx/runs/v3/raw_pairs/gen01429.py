from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.min("duration_sec").over(w).alias("session_id_min_duration_sec"),
    )
)
