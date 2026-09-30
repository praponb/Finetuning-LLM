from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time"), "page").orderBy("duration_sec", "id")
w_part = Window.partitionBy(F.year("event_time"), "session_id")
w3 = Window.orderBy("duration_sec", "id")
w4 = Window.orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("event_type").orderBy("duration_sec", "id")

with_windows = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.row_number().over(w).alias("row_seq"),
        F.sum("duration_sec").over(w_part).alias("event_time_year_sum_duration_sec"),
        F.ntile(3).over(w3).alias("duration_sec_bucket"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.lead("duration_sec", 2).over(w5).alias("next_duration_sec"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("next_duration_sec") >= 427, "LARGE").otherwise("SMALL").alias("next_duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.percentile_approx("next_duration_sec", 0.95).alias("next_duration_sec_p95"),
    )
    .filter(F.col("row_seq_p95") < 1000000)
    .orderBy("num_event_types")
)
