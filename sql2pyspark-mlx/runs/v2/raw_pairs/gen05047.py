from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/checkout", "/search"]))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
)
