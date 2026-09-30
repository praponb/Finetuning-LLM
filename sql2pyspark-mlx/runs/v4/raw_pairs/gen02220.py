from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-13 10:00:00").cast("timestamp")) & F.col("page").isNotNull() & (F.col("duration_sec") > 467))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

result = (
    base
    .select(
        "*",
        F.first("page", ignorenulls=True).over(w).alias("first_known_page"),
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
    )
)
