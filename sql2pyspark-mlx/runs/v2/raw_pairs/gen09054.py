from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        F.max("duration_sec").over(w).alias("page_max_duration_sec"),
        F.row_number().over(w2).alias("row_num"),
    )
)
