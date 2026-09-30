from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

result = (
    df_web_events
    .select("id", "session_id", F.count("*").over(w).alias("session_id_count"))
)
