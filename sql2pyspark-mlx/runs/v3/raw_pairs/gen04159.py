from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        "page",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.row_number().over(w2).alias("row_num"),
    )
)
