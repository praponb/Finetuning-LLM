from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "page", "event_time", "duration_sec")
)
