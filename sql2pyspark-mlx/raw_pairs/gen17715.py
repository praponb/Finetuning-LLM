from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("session_id").orderBy(F.col("running_max_event_time_day").asc_nulls_first())
w4 = Window.orderBy(F.col("duration_sec_whole").desc(), "id")
w5 = Window.partitionBy("session_id")
w6 = Window.partitionBy("browser").orderBy(F.col("session_id_max_duration_sec_whole_avg_5_prior").desc_nulls_first(), "id")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-10-02")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.dayofmonth("event_time").alias("event_time_day"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "session_id",
        "duration_sec_whole",
        "event_time_day",
        F.avg("event_time_day").over(w).alias("running_avg_event_time_day"),
        F.count("event_time_day").over(w).alias("running_count_event_time_day"),
        F.max("event_time_day").over(w).alias("running_max_event_time_day"),
        F.avg("duration_sec_whole").over(w2).alias("duration_sec_whole_avg_5_prior"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w3).alias("running_max_event_time_day_percent_rank"),
        F.row_number().over(w4).alias("row_seq"),
        F.max("duration_sec_whole_avg_5_prior").over(w5).alias("session_id_max_duration_sec_whole_avg_5_prior"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.started_at") >= "2024-12-11")
    .select(
        "s.id",
        "s.session_id",
        "s.duration_sec_whole",
        "s.event_time_day",
        "s.running_count_event_time_day",
        "s.running_max_event_time_day",
        "s.running_max_event_time_day_percent_rank",
        "s.row_seq",
        "s.session_id_max_duration_sec_whole_avg_5_prior",
        F.col("p.browser"),
        F.col("p.device"),
    )
)

cleaned_web_events = (
    joined
    .filter(F.col("browser").isNotNull())
    .select(
        "*",
        (F.col("session_id_max_duration_sec_whole_avg_5_prior") - F.col("running_max_event_time_day")).alias("session_id_max_duration_sec_whole_avg_5_prior_minus_running_max_event_time_day"),
    )
)

deduped = (
    cleaned_web_events
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .groupBy(
        "browser",
        F.when(F.col("running_count_event_time_day") >= 17, "PREMIUM").otherwise("STANDARD").alias("running_count_event_time_day_tier"),
    )
    .agg(
        F.avg(F.when(F.col("device") == "mobile", F.col("row_seq"))).alias("mobile_avg_row_seq"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("running_max_event_time_day")).otherwise(0)).alias("chrome_running_max_event_time_day"),
        F.sum(F.when(F.col("browser") == "Safari", F.col("running_max_event_time_day_percent_rank")).otherwise(0)).alias("safari_running_max_event_time_day_percent_rank"),
    )
    .orderBy(F.col("mobile_rows").desc_nulls_first())
)
