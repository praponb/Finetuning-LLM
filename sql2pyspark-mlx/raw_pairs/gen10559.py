from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_last(), "id")
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w5 = Window.orderBy("event_time", "id")
w6 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_sum_90d").asc_nulls_first(), "id")
w7 = Window.partitionBy("country", "session_id").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-03-27"))) & F.col("session_id").isin([12, 21, 33, 35]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "session_id", "page", "event_time", "duration_sec")
)

ranked_web_events = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    ranked_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.sum("duration_sec").over(w3).alias("duration_sec_sum_90d"),
        F.ntile(4).over(w4).alias("duration_sec_bucket"),
        F.lag("duration_sec", 1, 0).over(w5).alias("prev_duration_sec"),
    )
)

ranked_web_events_2 = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked_web_events_2
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.device"), F.col("b.country"))
)

result = (
    with_dims
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "duration_sec_sum_90d",
        "duration_sec_bucket",
        "prev_duration_sec",
        F.first("duration_sec_sum_90d", ignorenulls=True).over(w7).alias("first_known_duration_sec_sum_90d"),
        F.sum("prev_duration_sec").over(w8).alias("cumulative_prev_duration_sec"),
    )
)
