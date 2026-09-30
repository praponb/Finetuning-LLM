from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select("id", "session_id", "duration_sec", F.row_number().over(w).alias("row_num"))
)
