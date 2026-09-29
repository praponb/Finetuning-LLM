from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-07-11")
    .select(
        "id",
        "event_type",
        "event_time",
        F.dense_rank().over(w).alias("event_time_dense_rank"),
    )
)
