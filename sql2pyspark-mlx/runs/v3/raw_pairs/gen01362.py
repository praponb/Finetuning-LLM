from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.col("session_id") == 23)
    .select("id", "duration_sec", F.rank().over(w).alias("duration_sec_rank"))
    .orderBy(F.col("duration_sec_rank").desc())
)
