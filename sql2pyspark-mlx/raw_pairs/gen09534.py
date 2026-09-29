from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id").orderBy("event_time_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page")
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.partitionBy("event_type").orderBy("duration_sec_rounded")
w5 = Window.partitionBy("event_type").orderBy("page_avg_duration_sec_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("page_avg_duration_sec_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("event_type").orderBy("duration_sec_rounded_avg_7d", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy(F.col("first_known_page_avg_duration_sec_rounded").desc(), "id")
w9 = Window.orderBy(F.col("first_known_page_avg_duration_sec_rounded").desc(), "id")
w10 = Window.partitionBy("event_type").orderBy("running_avg_duration_sec_rounded_avg_7d", "id").rowsBetween(-3, -1)
w11 = Window.orderBy("duration_sec_rounded_avg_7d", "id")
w12 = Window.partitionBy("event_type").orderBy(F.col("running_avg_duration_sec_rounded_avg_7d").desc())
w13 = Window.partitionBy("event_type").orderBy("running_count_duration_sec_rounded_avg_7d_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("event_type").orderBy("duration_sec_rounded_avg_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy("event_type")
w16 = Window.partitionBy("event_type").orderBy(F.col("running_count_duration_sec_rounded_avg_7d").desc(), "id")
w17 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w18 = Window.partitionBy("event_type").orderBy("first_known_duration_sec_rounded_avg_7d", "id").rowsBetween(-6, Window.currentRow)
w19 = Window.partitionBy("event_type").orderBy("running_count_duration_sec_rounded_avg_7d_avg_3_prior", "id")
w20 = Window.partitionBy("event_type").orderBy("first_known_page_avg_duration_sec_rounded", "id")
w21 = Window.partitionBy("event_type").orderBy("running_avg_duration_sec_rounded_avg_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-06-20 03:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.hour("event_time").alias("event_time_hour"),
    )
)

enriched = (
    base
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "event_type",
        "duration_sec",
        "event_time_hour",
        F.last("page", ignorenulls=True).over(w_running).alias("last_known_page"),
        F.avg("duration_sec_rounded").over(w2).alias("page_avg_duration_sec_rounded"),
        F.avg("duration_sec_rounded").over(w3).alias("duration_sec_rounded_avg_7d"),
        F.cume_dist().over(w_recent).alias("duration_sec_rounded_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "event_type",
        "duration_sec",
        "duration_sec_rounded_avg_7d",
        F.sum("duration_sec_rounded_avg_7d").over(w5).alias("running_sum_duration_sec_rounded_avg_7d"),
        F.avg("duration_sec_rounded_avg_7d").over(w5).alias("running_avg_duration_sec_rounded_avg_7d"),
        F.count("duration_sec_rounded_avg_7d").over(w5).alias("running_count_duration_sec_rounded_avg_7d"),
        F.first("page_avg_duration_sec_rounded", ignorenulls=True).over(w6).alias("first_known_page_avg_duration_sec_rounded"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.avg("running_avg_duration_sec_rounded_avg_7d").over(w7).alias("running_avg_duration_sec_rounded_avg_7d_avg_2"),
        F.row_number().over(w8).alias("seq_no"),
        F.row_number().over(w9).alias("row_seq"),
        F.avg("running_count_duration_sec_rounded_avg_7d").over(w10).alias("running_count_duration_sec_rounded_avg_7d_avg_3_prior"),
        F.lag("first_known_page_avg_duration_sec_rounded", 1).over(w11).alias("prev_first_known_page_avg_duration_sec_rounded"),
    )
)

with_windows_web_events = (
    scored
    .withColumn("running_avg_duration_sec_rounded_avg_7d_percent_rank", F.percent_rank().over(w12))
    .withColumn("first_known_running_avg_duration_sec_rounded_avg_7d", F.first("running_avg_duration_sec_rounded_avg_7d", ignorenulls=True).over(w13))
    .withColumn("first_known_duration_sec_rounded_avg_7d", F.first("duration_sec_rounded_avg_7d", ignorenulls=True).over(w14))
    .withColumn("event_type_count_running_avg_duration_sec_rounded_avg_7d_avg_2", F.count("running_avg_duration_sec_rounded_avg_7d_avg_2").over(w15))
    .withColumn("row_seq_2", F.row_number().over(w16))
)

latest = (
    with_windows_web_events
    .withColumn("dedup_rank", F.rank().over(w17))
    .filter(F.col("dedup_rank") == 1)
)

with_windows_web_events_2 = (
    latest
    .select(
        "id",
        "event_type",
        "duration_sec",
        "running_sum_duration_sec_rounded_avg_7d",
        "running_avg_duration_sec_rounded_avg_7d",
        "running_count_duration_sec_rounded_avg_7d",
        "first_known_page_avg_duration_sec_rounded",
        "row_seq",
        "running_count_duration_sec_rounded_avg_7d_avg_3_prior",
        "running_avg_duration_sec_rounded_avg_7d_percent_rank",
        "first_known_duration_sec_rounded_avg_7d",
        "dedup_rank",
        F.count("duration_sec_rounded_avg_7d").over(w18).alias("last7_count_duration_sec_rounded_avg_7d"),
        F.sum("duration_sec_rounded_avg_7d").over(w18).alias("last7_sum_duration_sec_rounded_avg_7d"),
        F.avg("duration_sec_rounded_avg_7d").over(w18).alias("last7_avg_duration_sec_rounded_avg_7d"),
        F.ntile(5).over(w19).alias("running_count_duration_sec_rounded_avg_7d_avg_3_prior_bucket"),
    )
)

result = (
    with_windows_web_events_2
    .select(
        "id",
        "event_type",
        "duration_sec",
        "running_avg_duration_sec_rounded_avg_7d",
        "first_known_page_avg_duration_sec_rounded",
        "row_seq",
        "running_avg_duration_sec_rounded_avg_7d_percent_rank",
        "first_known_duration_sec_rounded_avg_7d",
        "dedup_rank",
        "last7_count_duration_sec_rounded_avg_7d",
        "last7_sum_duration_sec_rounded_avg_7d",
        F.lag("last7_avg_duration_sec_rounded_avg_7d").over(w20).alias("prev_last7_avg_duration_sec_rounded_avg_7d"),
        F.lead("running_avg_duration_sec_rounded_avg_7d").over(w20).alias("next_running_avg_duration_sec_rounded_avg_7d"),
        F.row_number().over(w20).alias("seq_no"),
        F.sum("running_count_duration_sec_rounded_avg_7d").over(w21).alias("running_running_count_duration_sec_rounded_avg_7d"),
    )
)
