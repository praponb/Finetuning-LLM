from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("event_time")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w_recent = Window.partitionBy(F.year("event_time")).orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec_bucket").desc(), "id")
w_hist = Window.partitionBy("session_id", "page").orderBy("cumulative_duration_sec", "id")
w6 = Window.partitionBy("session_id").orderBy("duration_sec_sum_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w2))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w_recent))
)

cleaned = (
    enriched
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "cumulative_duration_sec",
        "duration_sec_bucket",
        "duration_sec_sum_90d",
        (F.col("cumulative_duration_sec") * F.col("duration_sec_bucket")).alias("cumulative_duration_sec_times_duration_sec_bucket"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    ranked
    .filter(F.col("cumulative_duration_sec_times_duration_sec_bucket") > 39)
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "duration_sec_bucket",
        "duration_sec_sum_90d",
        "cumulative_duration_sec_times_duration_sec_bucket",
        F.lead("cumulative_duration_sec").over(w_hist).alias("next_cumulative_duration_sec"),
        F.lag("cumulative_duration_sec").over(w_hist).alias("prev_cumulative_duration_sec"),
        F.lag("duration_sec_bucket").over(w_hist).alias("prev_duration_sec_bucket"),
        F.lead("duration_sec_bucket").over(w_hist).alias("next_duration_sec_bucket"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.sum("duration_sec_sum_90d").over(w6).alias("cumulative_duration_sec_sum_90d"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.device"), F.col("p2.country"))
)

joined_web_events = (
    joined
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.device").alias("session_device"),
        F.col("p.country").alias("session_country"),
        F.col("p.browser"),
    )
)

joined_web_events_2 = (
    joined_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.duration_sec_sum_90d",
        "e.cumulative_duration_sec_times_duration_sec_bucket",
        "e.prev_cumulative_duration_sec",
        "e.prev_duration_sec_bucket",
        "e.row_seq",
        "e.cumulative_duration_sec_sum_90d",
        "e.device",
        "e.session_country",
        F.col("d.browser").alias("session_browser"),
        F.col("d.country").alias("session_country_2"),
        F.col("d.device").alias("session_device_2"),
    )
)

result = (
    joined_web_events_2
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .rollup(
        F.col("s.country").alias("country"),
        F.col("e.session_device_2").alias("session_device_2"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.cumulative_duration_sec_sum_90d")).alias("max_cumulative_duration_sec_sum_90d"),
    )
    .orderBy(F.col("max_cumulative_duration_sec_sum_90d").asc_nulls_last())
)
