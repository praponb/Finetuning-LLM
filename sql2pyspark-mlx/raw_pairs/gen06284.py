from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("event_time")).orderBy(F.col("event_time").desc(), "id")
w_hist = Window.partitionBy("session_id")

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-06-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.row_number().over(w).alias("seq_no"),
        F.sum("duration_sec").over(w_hist).alias("session_id_sum_duration_sec"),
    )
)
