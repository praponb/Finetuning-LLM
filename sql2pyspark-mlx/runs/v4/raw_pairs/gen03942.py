from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-05-01")
    .select(
        "id",
        "event_type",
        "event_time",
        "session_id",
        F.rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
)
