from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "session_id",
        F.last("session_id").over(w).alias("last_session_id"),
    )
)
