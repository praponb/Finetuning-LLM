from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec_max_90d").desc(), "id")
w4 = Window.orderBy(F.col("next_duration_sec").desc(), "id")
w5 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy("page").orderBy(F.col("next_duration_sec").desc_nulls_last())
w_part = Window.orderBy(F.col("duration_sec_max_90d").desc(), "id")
w8 = Window.partitionBy(F.to_date("event_time"))

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/cart"))) & (F.col("event_time") >= "2024-10-01"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
    )
)

scored = (
    valid
    .select(
        "id",
        "page",
        "event_time",
        "event_time_label",
        F.lead("duration_sec").over(w).alias("next_duration_sec"),
        F.row_number().over(w).alias("row_seq"),
        F.max("duration_sec").over(w2).alias("duration_sec_max_90d"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .filter(F.col("next_duration_sec").between(201, 422))
    .select(
        "id",
        "page",
        "duration_sec_max_90d",
        F.ntile(3).over(w4).alias("next_duration_sec_bucket"),
        F.max("row_seq").over(w5).alias("row_seq_max_30d"),
        F.cume_dist().over(w6).alias("next_duration_sec_cume_dist"),
        F.ntile(5).over(w_part).alias("duration_sec_max_90d_bucket"),
        F.min("row_seq").over(w8).alias("event_time_to_date_min_row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "duration_sec_max_90d",
        "next_duration_sec_bucket",
        "next_duration_sec_cume_dist",
        "duration_sec_max_90d_bucket",
        "event_time_to_date_min_row_seq",
    )
    .orderBy(F.col("duration_sec_max_90d").desc())
)
