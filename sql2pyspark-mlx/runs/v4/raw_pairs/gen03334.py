from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("page_avg_duration_sec"),
        F.ntile(10).over(w2).alias("duration_sec_bucket"),
    )
)
