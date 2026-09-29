from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())
w2 = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "page",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("row_num"),
    )
)
