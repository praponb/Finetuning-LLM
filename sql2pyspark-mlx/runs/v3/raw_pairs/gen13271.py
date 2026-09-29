from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec").between(210, 216))
    .select("id", "page", "duration_sec", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("rn").desc())
)
