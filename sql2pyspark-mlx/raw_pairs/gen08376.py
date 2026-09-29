from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy(F.to_date("event_time"), "page").orderBy("duration_sec", "id")
w4 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("event_type").orderBy("event_time", "id")
w7 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_bucket_2").desc(), "id")

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-08-28").cast("date"))
    .withColumn("event_time_date", F.to_date("event_time"))
    .withColumn("clean_page", F.trim("page"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

with_windows = (
    filtered
    .filter(F.col("event_time") < "2025-08-01")
    .select(
        "id",
        "event_type",
        "event_time",
        "event_time_date",
        "clean_page",
        "resolved_page",
        F.lag("duration_sec", 1, 0).over(w).alias("prev_duration_sec"),
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
        F.ntile(4).over(w3).alias("duration_sec_bucket_2"),
        F.lead("duration_sec", 2, 0).over(w4).alias("next_duration_sec"),
        F.avg("duration_sec").over(w5).alias("duration_sec_avg_6_prior"),
    )
)

flagged = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "event_type",
        "event_time",
        "event_time_date",
        "clean_page",
        "resolved_page",
        "prev_duration_sec",
        "duration_sec_bucket_2",
        "next_duration_sec",
        "duration_sec_avg_6_prior",
        F.sum("new_group_flag").over(w7).alias("session_no"),
    )
)

latest = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time_date"))
    .groupingSets(
        [["activity_month", "event_type"], ["activity_month"]],
        "activity_month",
        "event_type",
    )
    .agg(
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.round(F.avg("prev_duration_sec"), 2).alias("avg_prev_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("activity_month").desc())
)
