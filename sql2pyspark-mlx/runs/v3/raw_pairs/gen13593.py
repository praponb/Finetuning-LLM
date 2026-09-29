from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page")
w2 = Window.partitionBy("session_id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        "session_id",
        "duration_sec",
        F.count("*").over(w).alias("event_type_count"),
        F.max("duration_sec").over(w2).alias("session_id_max_duration_sec"),
    )
    .orderBy(F.col("session_id_max_duration_sec").desc(), F.col("event_type").desc())
)
