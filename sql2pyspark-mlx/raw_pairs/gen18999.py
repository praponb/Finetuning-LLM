from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        "event_time",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "event_type", "duration_sec", "event_time")
    .orderBy("event_type")
)
