from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy(F.date_trunc("month", "event_time"), "page")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "page",
        "duration_sec",
        F.count("*").over(w).alias("event_type_count"),
        F.min("duration_sec").over(w2).alias("event_time_period_min_duration_sec"),
    )
)
