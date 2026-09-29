from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "page",
        F.last("page").over(w).alias("last_page"),
        F.percent_rank().over(w2).alias("event_time_pct_rank"),
    )
)
