from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("event_type") != "purchase")
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy("event_time")
)
