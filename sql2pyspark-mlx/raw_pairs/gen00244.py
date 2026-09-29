from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

scored = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product", "/home"]))
    .select(
        "*",
        F.lead("duration_sec").over(w).alias("next_duration_sec"),
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("seq_no"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
    )
)

result = (
    scored
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "prev_duration_sec",
        "seq_no",
        "last_known_duration_sec",
        F.col("avg_web_event_duration_sec"),
    )
)
