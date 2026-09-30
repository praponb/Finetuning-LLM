from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_group = Window.partitionBy("event_type_group").orderBy("event_at", "id")
w_event_type_group_running = Window.partitionBy("event_type_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_group_2 = Window.partitionBy("event_type_group")
w_event_type_group_event_count = Window.partitionBy("event_type_group", "event_count").orderBy("session_start", "event_type_group", "session_no").rowsBetween(-2, Window.currentRow)
w_session_no_session_start_mon = Window.partitionBy("session_no", F.date_trunc("month", "session_start")).orderBy(F.col("event_count").desc())

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "replied"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_event_type_group))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_event_type_group_running))

session_summary = (
    sessionized
    .groupBy("event_type_group", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered_2 = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("session_no").alias("overall_avg_session_no")))
    .filter(F.col("session_no") > F.col("overall_avg_session_no"))
    .drop("overall_avg_session_no")
)

sequenced = (
    filtered_2
    .select(
        "event_type_group",
        "session_no",
        "session_start",
        "session_end",
        "duration_min",
        F.count(F.lit(1)).over(w_event_type_group_2).alias("rows_in_event_type_group"),
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_event_type_group_2), 2).alias("duration_min_vs_event_type_group_avg"),
        F.round(F.avg("session_no").over(w_event_type_group_event_count), 2).alias("session_no_avg_3"),
        F.sum("event_count").over(w_event_type_group_2).alias("event_type_group_sum_event_count"),
        F.rank().over(w_session_no_session_start_mon).alias("event_count_rank"),
    )
)

result = (
    sequenced
    .select(
        "event_type_group",
        "session_no",
        "session_start",
        "session_end",
        "duration_min",
        "duration_min_vs_event_type_group_avg",
        "session_no_avg_3",
        "event_type_group_sum_event_count",
        "event_count_rank",
    )
    .orderBy(F.col("session_no").asc_nulls_last(), "event_type_group")
    .limit(20)
)
