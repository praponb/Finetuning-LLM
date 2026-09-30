from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time").desc())
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
