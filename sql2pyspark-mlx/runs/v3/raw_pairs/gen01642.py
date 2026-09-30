from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([15, 21, 34, 40]))
    .select("id", "duration_sec", F.row_number().over(w).alias("row_num"))
)
