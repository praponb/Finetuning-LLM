from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
