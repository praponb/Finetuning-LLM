from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .select("id", "session_id", "event_time", F.ntile(10).over(w).alias("quartile"))
    .orderBy(F.col("quartile").desc())
)
