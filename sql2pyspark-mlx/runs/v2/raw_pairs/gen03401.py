from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/checkout"]))
    .select(
        "id",
        "event_type",
        "event_time",
        "session_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "event_type", "event_time", "session_id")
)
