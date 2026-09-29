from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "*",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.last("page", ignorenulls=True).over(w2).alias("last_known_page"),
        F.last("duration_sec", ignorenulls=True).over(w_hist).alias("last_known_duration_sec"),
    )
)
