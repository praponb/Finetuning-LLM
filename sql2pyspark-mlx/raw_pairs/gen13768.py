from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").desc_nulls_first())
w2 = Window.orderBy("duration_sec", "id")
w3 = Window.partitionBy(F.year("event_time"), "page")
w4 = Window.partitionBy(F.year("event_time")).orderBy(F.col("duration_sec").desc_nulls_first())
w5 = Window.partitionBy("page").orderBy(F.col("duration_sec_dense_rank").desc_nulls_first(), "id")
w_part = Window.partitionBy("page").orderBy("duration_sec_dense_rank", "id")
w7 = Window.partitionBy("page").orderBy("event_time_year_avg_duration_sec", "id")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.to_timestamp(F.lit("2023-04-16 07:00:00")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.trim("page").alias("clean_page"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "clean_page",
    )
)

with_windows = (
    ranked
    .filter(F.col("page").isin(["/search", "/product"]))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "clean_page",
        F.row_number().over(w2).alias("seq_no"),
        F.avg("duration_sec").over(w3).alias("event_time_year_avg_duration_sec"),
        F.dense_rank().over(w4).alias("duration_sec_dense_rank"),
    )
)

ranked_web_events = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked_web_events
    .select(
        "id",
        "page",
        "seq_no",
        F.lag("duration_sec_dense_rank").over(w_part).alias("prev_duration_sec_dense_rank"),
        F.lead("duration_sec").over(w_part).alias("next_duration_sec"),
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(5).over(w7).alias("event_time_year_avg_duration_sec_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "page",
        "seq_no",
        "prev_duration_sec_dense_rank",
        "next_duration_sec",
        "row_seq",
        "event_time_year_avg_duration_sec_bucket",
    )
    .orderBy(
        F.col("event_time_year_avg_duration_sec_bucket").desc_nulls_first(),
        "seq_no",
    )
)
