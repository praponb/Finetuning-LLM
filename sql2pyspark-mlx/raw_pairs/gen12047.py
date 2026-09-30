from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/home"]))
    .select("id", "session_id", "page", "event_time", F.rank().over(w).alias("rnk"))
)
