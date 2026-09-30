from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        "event_type",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "page", "duration_sec", "event_type")
    .orderBy("event_type")
)
