from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "session_id",
        F.rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "event_type", "event_time", "session_id")
)
