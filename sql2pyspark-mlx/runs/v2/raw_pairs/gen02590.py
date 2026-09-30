from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.max("duration_sec").over(w).alias("session_id_max_duration_sec"),
    )
)
