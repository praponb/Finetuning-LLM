from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = df_web_events.select("session_id", "event_time", F.lag("page", 1).over(w).alias("prev_page"))
