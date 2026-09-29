from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "page",
        "duration_sec",
        F.min("duration_sec").over(w).alias("page_min_duration_sec"),
    )
    .orderBy("page_min_duration_sec", "page")
)
