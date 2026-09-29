from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("event_time")).orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        (F.col("duration_sec") - F.lag("duration_sec", 2).over(w)).alias("duration_sec_change"),
    )
)
