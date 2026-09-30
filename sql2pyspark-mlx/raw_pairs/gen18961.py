from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("duration_sec_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_time", "id")
w4 = Window.orderBy("duration_sec_adjusted", "id")
w5 = Window.partitionBy("event_type")
w_grp = Window.partitionBy("page", "session_id").orderBy("next_duration_sec_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device", "session_id").orderBy("running_avg_cumulative_duration_sec", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.orderBy(F.col("cumulative_duration_sec").desc(), "id")
w9 = Window.orderBy("running_sum_cumulative_duration_sec", "id")
w_part = Window.partitionBy("page").orderBy("cumulative_duration_sec", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.partitionBy("session_country").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 1.07), 2).alias("duration_sec_adjusted"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec_adjusted",
        F.last("page", ignorenulls=True).over(w_hist).alias("last_known_page"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.row_number().over(w3).alias("seq_no"),
        F.lead("duration_sec_adjusted", 1, 0).over(w4).alias("next_duration_sec_adjusted"),
        F.stddev_samp("duration_sec_adjusted").over(w5).alias("event_type_stddev_samp_duration_sec_adjusted"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.cumulative_duration_sec",
        "e.seq_no",
        "e.next_duration_sec_adjusted",
        F.col("b.country"),
    )
)

with_windows_web_events = (
    joined
    .withColumn("running_count_cumulative_duration_sec", F.count("cumulative_duration_sec").over(w_grp))
    .withColumn("running_avg_cumulative_duration_sec", F.avg("cumulative_duration_sec").over(w_grp))
    .withColumn("running_sum_cumulative_duration_sec", F.sum("cumulative_duration_sec").over(w_grp))
)

joined_web_events = (
    with_windows_web_events
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.cumulative_duration_sec",
        "s.seq_no",
        "s.country",
        "s.running_avg_cumulative_duration_sec",
        "s.running_sum_cumulative_duration_sec",
        F.col("p2.device"),
        F.col("p2.country").alias("session_country"),
    )
)

enriched = (
    joined_web_events
    .select(
        "id",
        "page",
        "cumulative_duration_sec",
        "country",
        "running_sum_cumulative_duration_sec",
        "device",
        "session_country",
        F.avg("running_sum_cumulative_duration_sec").over(w7).alias("running_sum_cumulative_duration_sec_avg_5"),
        F.row_number().over(w8).alias("event_seq"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.count("event_seq").over(w_part).alias("last7_count_event_seq"),
        F.max("event_seq").over(w_part).alias("last7_max_event_seq"),
        F.avg("event_seq").over(w_part).alias("last7_avg_event_seq"),
        F.sum("running_sum_cumulative_duration_sec").over(w11).alias("cumulative_running_sum_cumulative_duration_sec"),
    )
)

result = (
    metrics
    .select(
        "id",
        "page",
        "cumulative_duration_sec",
        "country",
        "running_sum_cumulative_duration_sec",
        "device",
        "running_sum_cumulative_duration_sec_avg_5",
        "event_seq",
        "last7_count_event_seq",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "PRIORITY").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy("cumulative_duration_sec")
)
