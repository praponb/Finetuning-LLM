from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("event_type").orderBy("session_no")
w5 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")
w6 = Window.orderBy(F.col("prev_event_time").desc(), "id")
w7 = Window.partitionBy("session_id").orderBy("prev_event_time", "id")
w8 = Window.partitionBy("page").orderBy("last7_sum_duration_sec", "id")
w9 = Window.partitionBy("row_seq_tier").orderBy(F.col("avg_last7_avg_duration_sec").desc())

with_windows = (
    df_web_events
    .select(
        "*",
        F.count("duration_sec").over(w).alias("last7_count_duration_sec"),
        F.avg("duration_sec").over(w).alias("last7_avg_duration_sec"),
        F.sum("duration_sec").over(w).alias("last7_sum_duration_sec"),
    )
)

with_prev = with_windows.withColumn("prev_event_time", F.lag("event_time").over(w2))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("new_group_flag").over(w3))

metrics = (
    islands
    .select(
        "id",
        "session_id",
        "duration_sec",
        "last7_count_duration_sec",
        "last7_avg_duration_sec",
        "last7_sum_duration_sec",
        F.rank().over(w_grp).alias("session_no_rank"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w7).alias("seq_no_2"),
        F.lag("last7_sum_duration_sec", 1).over(w8).alias("prev_last7_sum_duration_sec"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .filter(F.col("p.device") == "tablet")
    .select("a.*", F.col("p.country"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.device"))
)

daily = (
    joined
    .groupBy(
        F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        F.when(F.col("duration_sec") >= 500, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("last7_avg_duration_sec"), 2).alias("avg_last7_avg_duration_sec"),
        F.avg("last7_count_duration_sec").alias("avg_last7_count_duration_sec"),
        F.count("*").alias("row_count"),
    )
    .withColumn("avg_last7_avg_duration_sec_rank", F.rank().over(w9))
)

result = daily.select("row_seq_tier", "duration_sec_tier", "avg_last7_count_duration_sec")
