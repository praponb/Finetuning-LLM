from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w2 = Window.partitionBy("event_type")
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.row_number().over(w).alias("event_seq"),
        F.count("duration_sec").over(w2).alias("event_type_count_duration_sec"),
        F.ntile(4).over(w3).alias("duration_sec_bucket"),
    )
)
