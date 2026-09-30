from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time"), "session_id").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "event_type").orderBy(F.col("duration_sec").desc())

metrics = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.row_number().over(w).alias("seq_no"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_5"),
        F.rank().over(w3).alias("duration_sec_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "seq_no",
        "duration_sec_avg_5",
        "duration_sec_rank",
    )
)
