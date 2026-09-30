from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home"]))
    .select(
        "id",
        "page",
        "event_type",
        "duration_sec",
        "event_time",
        F.first("event_time").over(w).alias("first_event_time"),
    )
)
