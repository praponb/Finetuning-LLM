from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.row_number().over(w).alias("row_num"),
    )
    .orderBy("id")
)
