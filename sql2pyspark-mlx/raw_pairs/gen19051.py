from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
