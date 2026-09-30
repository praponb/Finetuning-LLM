from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select("id", "session_id", "duration_sec", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
