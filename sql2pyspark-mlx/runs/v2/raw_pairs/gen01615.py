from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.col("session_id") == 4)
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "session_id", "event_time", "event_type")
)
