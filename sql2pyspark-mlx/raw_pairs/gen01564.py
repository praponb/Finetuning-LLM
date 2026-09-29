from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc())
w2 = Window.orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
    )
)
