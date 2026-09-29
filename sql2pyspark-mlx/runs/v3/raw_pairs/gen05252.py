from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_type",
        "event_time",
        F.percent_rank().over(w).alias("duration_sec_pct_rank"),
        F.rank().over(w2).alias("event_time_rank"),
    )
    .orderBy(F.col("event_time_rank").desc())
)
