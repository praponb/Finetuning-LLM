from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")
w2 = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("duration_sec").between(223, 427))
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        F.rank().over(w).alias("event_time_rank"),
        F.rank().over(w2).alias("event_time_rank_2"),
    )
)
