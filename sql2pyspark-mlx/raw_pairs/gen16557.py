from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.last("page", ignorenulls=True).over(w_recent).alias("last_known_page"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
    )
)
