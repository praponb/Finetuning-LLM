from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select("id", "page", "duration_sec", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "page", "duration_sec")
)
