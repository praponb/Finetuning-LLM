from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "*",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
    )
)
