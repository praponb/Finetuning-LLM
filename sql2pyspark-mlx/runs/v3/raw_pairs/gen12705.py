from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        F.percent_rank().over(w).alias("event_time_pct_rank"),
    )
    .orderBy("event_time")
)
