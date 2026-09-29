from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec_whole", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.partitionBy("page").orderBy("event_time_date", "id")
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_part = Window.partitionBy("page").orderBy(F.col("event_time_date").desc(), "id")
w6 = Window.partitionBy("page").orderBy("duration_sec_whole_avg_7d", "id")
w7 = Window.partitionBy("page")
w8 = Window.partitionBy("page").orderBy("event_time", "id")
w9 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "page").orderBy("session_start_flag", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.partitionBy("page").orderBy(F.col("is_new_burst").asc_nulls_last())

valid = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-01").cast("date"))
    .select(
        "id",
        "page",
        "event_time",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.lag("duration_sec_whole").over(w).alias("prev_duration_sec_whole"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("duration_sec_whole").over(w2).alias("duration_sec_whole_avg_7d"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "page",
        "event_time",
        "event_time_date",
        "page_clean",
        "prev_duration_sec_whole",
        "row_seq",
        "duration_sec_whole_avg_7d",
        F.lead("prev_duration_sec_whole", 1).over(w_recent).alias("next_prev_duration_sec_whole"),
        F.sum("duration_sec_whole_avg_7d").over(w4).alias("duration_sec_whole_avg_7d_sum_7d"),
        F.row_number().over(w_part).alias("event_seq"),
        F.lead("duration_sec_whole", 2).over(w6).alias("next_duration_sec_whole"),
        F.count("duration_sec_whole").over(w7).alias("page_count_duration_sec_whole"),
    )
)

with_gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w8).isNull() | (F.col("event_time") > F.lag("event_time").over(w8) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "page",
        "event_time",
        "event_time_date",
        "page_clean",
        "row_seq",
        "page_count_duration_sec_whole",
        F.sum("session_start_flag").over(w9).alias("burst_id"),
    )
)

flagged = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w8).isNull() | (F.col("event_time") > F.lag("event_time").over(w8) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("is_new_burst").over(w9))

prev_events = sessions_web_events.withColumn("prev_event_time", F.lag("event_time").over(w8))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized_web_events = gaps.withColumn("burst_id_2", F.sum("session_start_flag").over(w9))

with_windows = (
    sessionized_web_events
    .filter(F.col("event_time_date") <= "2023-04-10")
    .select(
        "*",
        F.last("row_seq", ignorenulls=True).over(w9).alias("last_known_row_seq"),
        F.avg("island_id").over(w10).alias("island_id_avg_2"),
        F.row_number().over(w_part).alias("event_seq"),
        F.dense_rank().over(w11).alias("is_new_burst_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "page",
        "event_time_date",
        "page_clean",
        "row_seq",
        "page_count_duration_sec_whole",
        "is_new_burst",
        "island_id",
        "prev_event_time",
    )
)
