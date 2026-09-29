from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select("id", "duration_sec", F.rank().over(w).alias("duration_sec_rank"))
)
