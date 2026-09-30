from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        "session_id",
        F.rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .select("id", "event_type", "duration_sec", "session_id")
    .orderBy(F.col("duration_sec").desc())
)
