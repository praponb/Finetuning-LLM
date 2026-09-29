from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "page",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .select("id", "event_type", "event_time", "page")
)
