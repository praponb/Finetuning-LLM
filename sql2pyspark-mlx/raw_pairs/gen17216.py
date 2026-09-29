from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("event_type_avg_duration_sec"),
    )
)
