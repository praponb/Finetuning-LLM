from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([16, 20, 30]))
    .select(
        "id",
        "page",
        "duration_sec",
        "event_time",
        F.rank().over(w).alias("duration_sec_rank"),
        F.lead("duration_sec", 1).over(w2).alias("next_duration_sec"),
    )
)
