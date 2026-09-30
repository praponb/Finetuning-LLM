from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select("id", "event_type", "duration_sec", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("id").desc(), F.col("duration_sec").desc())
)
