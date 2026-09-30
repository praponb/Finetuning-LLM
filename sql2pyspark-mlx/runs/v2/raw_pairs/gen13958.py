from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

result = (
    df_web_events
    .filter(F.col("session_id") == 12)
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.max("duration_sec").over(w).alias("session_id_max_duration_sec"),
    )
)
