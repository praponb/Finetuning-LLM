from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select("id", "page", "duration_sec", F.ntile(10).over(w).alias("quartile"))
    .orderBy(F.col("page").desc())
)
