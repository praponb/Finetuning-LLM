from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home"]))
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 1)
    .select("id", "session_id", "event_time", "event_type")
    .orderBy(F.col("event_type").desc())
)
