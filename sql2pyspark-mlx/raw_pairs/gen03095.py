from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy("is_new_burst", "id")
w_grp = Window.partitionBy("session_id").orderBy("running_count_duration_sec", "id")
w6 = Window.partitionBy("session_id").orderBy(F.col("running_max_duration_sec").desc_nulls_last())
w7 = Window.partitionBy("session_id").orderBy(F.col("running_count_duration_sec").desc(), "id")

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

metrics = (
    sessionized
    .filter(F.col("burst_id") < 6)
    .withColumn("running_avg_duration_sec", F.avg("duration_sec").over(w2))
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("running_count_duration_sec", F.count("duration_sec").over(w2))
    .withColumn("running_max_duration_sec", F.max("duration_sec").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

with_windows = (
    islands
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "running_count_duration_sec",
        "running_max_duration_sec",
        "seq_no",
        F.lag("island_id", 2, 0).over(w4).alias("prev_island_id"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.dense_rank().over(w6).alias("running_max_duration_sec_dense_rank"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

flagged_2 = (
    ranked
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    flagged_2
    .select(
        "id",
        "session_id",
        "event_time",
        "running_max_duration_sec",
        "prev_island_id",
        "running_max_duration_sec_dense_rank",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

joined = (
    sessions_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.device"), F.col("d.country"), F.col("d.browser"))
)

result = (
    joined
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"), "browser")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .orderBy(F.col("browser").desc())
)
