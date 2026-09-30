from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type")

result = (
    df_web_events
    .filter(F.col("page") != "/home")
    .select(
        "id",
        "session_id",
        "event_type",
        F.count("*").over(w).alias("session_id_count"),
    )
)
