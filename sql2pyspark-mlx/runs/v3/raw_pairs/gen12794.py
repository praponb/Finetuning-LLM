from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("duration_sec").between(194, 324))
    .select("id", "event_time", F.row_number().over(w).alias("rn"))
)
