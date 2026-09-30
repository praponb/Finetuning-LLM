from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "page",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "event_type", "event_time", "page")
    .orderBy(F.col("id").desc())
)
