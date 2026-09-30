from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(~F.col("page").isin(["/search", "/home"]))
    .select("id", "page", "event_time", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "page", "event_time")
    .orderBy(F.col("event_time").desc())
)
