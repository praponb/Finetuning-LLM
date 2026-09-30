from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .select(
        "id",
        "event_type",
        "event_time",
        "session_id",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy("event_type")
)
