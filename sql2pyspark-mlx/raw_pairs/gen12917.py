from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec_filled").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w_recent = Window.orderBy(F.col("duration_sec").desc(), "id")
w_hist = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.to_timestamp(F.lit("2023-08-08 09:00:00"))))
    .select("*", F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "event_type", "duration_sec")
)

filtered = (
    ranked
    .filter(F.col("event_type") == "purchase")
    .withColumn("event_type_prefix", F.substring("event_type", 1, 2))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "duration_sec", "event_type_prefix")
)

with_windows = (
    latest
    .select(
        "id",
        "event_type_prefix",
        F.row_number().over(w_recent).alias("row_seq"),
        F.last("duration_sec", ignorenulls=True).over(w_hist).alias("last_known_duration_sec"),
        F.ntile(3).over(w_recent).alias("duration_sec_bucket"),
        F.sum("duration_sec").over(w_hist).alias("cumulative_duration_sec"),
    )
)

filtered_web_events = (
    with_windows
    .filter(F.col("row_seq") != 4)
    .select(
        "*",
        (F.col("duration_sec_bucket") - F.col("last_known_duration_sec")).alias("duration_sec_bucket_minus_last_known_duration_sec"),
    )
)

valid = (
    filtered_web_events
    .filter(F.col("duration_sec_bucket") < 6)
    .withColumn("row_seq_rounded", F.round("row_seq", 2))
    .withColumn("row_seq_times_cumulative_duration_sec", (F.col("row_seq") * F.col("cumulative_duration_sec")))
)

result = (
    valid
    .select(
        "id",
        "event_type_prefix",
        "row_seq",
        "last_known_duration_sec",
        "duration_sec_bucket",
        "cumulative_duration_sec",
        "duration_sec_bucket_minus_last_known_duration_sec",
        "row_seq_rounded",
        "row_seq_times_cumulative_duration_sec",
    )
    .orderBy("last_known_duration_sec")
)
