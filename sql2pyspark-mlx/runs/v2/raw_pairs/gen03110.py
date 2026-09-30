from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(~F.col("page").isin(["/home", "/product", "/search"]))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 1, 0).over(w).alias("next_duration_sec"),
    )
)
