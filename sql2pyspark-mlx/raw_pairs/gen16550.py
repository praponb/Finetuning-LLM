from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("page", ignorenulls=True).over(w3).alias("last_known_page"),
    )
)
