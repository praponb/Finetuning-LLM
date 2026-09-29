from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("event_time_rank"),
    )
)
