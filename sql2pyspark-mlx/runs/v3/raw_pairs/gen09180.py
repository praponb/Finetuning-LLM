from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 20, 25, 39]))
    .select(
        "id",
        "event_type",
        "event_time",
        F.percent_rank().over(w).alias("event_time_pct_rank"),
    )
)
