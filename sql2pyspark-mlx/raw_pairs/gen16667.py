from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.hour("event_time").between(11, 15))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "page",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "session_id", "duration_sec", "page")
    .orderBy("page", "duration_sec")
)
