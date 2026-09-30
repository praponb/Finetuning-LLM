from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.col("csat").between(1, 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

with_prev = base_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_priority_running))

session_summary = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = session_summary.withColumn("session_start_quarter", F.quarter(F.col("session_start")))

grouped = (
    tagged
    .groupBy(
        "burst_id",
        F.date_format("session_start", "yyyy-MM").alias("session_start_ym"),
        "session_start_quarter",
    )
    .agg(
        F.sum(F.when(F.col("priority").isin("P4", "P1"), 1).otherwise(0)).alias("n_priority_in_p4_p1"),
        F.sum("session_requester_id").alias("total_session_requester_id"),
        F.min("session_requester_id").alias("min_session_requester_id"),
        F.round(F.avg("session_start_quarter"), 2).alias("avg_session_start_quarter"),
        F.round(F.stddev("duration_min"), 2).alias("stddev_duration_min"),
        F.round(F.stddev("session_requester_id"), 2).alias("stddev_session_requester_id"),
        F.sum("event_count").alias("sum_event_count_check"),
    )
    .filter(F.col("sum_event_count_check") > 2)
    .drop("sum_event_count_check")
)

result = (
    grouped
    .select(
        "burst_id",
        "session_start_quarter",
        "n_priority_in_p4_p1",
        "total_session_requester_id",
        "min_session_requester_id",
        "avg_session_start_quarter",
        "stddev_duration_min",
        F.length(F.trim(F.col("session_start_ym"))).alias("session_start_ym_len"),
    )
    .orderBy("session_start_quarter")
)
