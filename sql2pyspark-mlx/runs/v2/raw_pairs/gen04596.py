from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "page", "duration_sec", "event_type")
    .orderBy("page", F.col("duration_sec").desc())
)
