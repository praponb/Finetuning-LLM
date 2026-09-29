from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "event_time"), "session_id").orderBy("duration_sec")
w2 = Window.partitionBy(F.month("event_time"), "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w4 = Window.partitionBy(F.to_date("event_time"), "event_type").orderBy(F.col("event_time").desc(), "id")
w5 = Window.orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        F.dense_rank().over(w).alias("duration_sec_dense_rank"),
        F.first("page", ignorenulls=True).over(w2).alias("first_known_page"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
        F.row_number().over(w4).alias("seq_no"),
        F.lead("duration_sec", 1).over(w5).alias("next_duration_sec_2"),
    )
)
