from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select("id", "page", "duration_sec", F.row_number().over(w).alias("rn"))
    .orderBy("page", F.col("duration_sec").desc())
)
