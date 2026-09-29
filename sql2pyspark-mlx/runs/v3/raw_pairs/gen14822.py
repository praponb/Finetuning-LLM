from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        "event_time",
        F.first("event_time").over(w).alias("first_event_time"),
    )
)
