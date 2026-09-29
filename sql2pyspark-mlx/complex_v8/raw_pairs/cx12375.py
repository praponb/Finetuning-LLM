from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_opened_at_to_date_event = Window.partitionBy(F.to_date("prev_opened_at"), "event_count")
w_new_session_flag_agent_id_by = Window.partitionBy("new_session_flag", "agent_id").orderBy(F.col("burst_id").desc(), "id")
w_new_session_flag_agent_id_by_2 = Window.partitionBy("new_session_flag", "agent_id").orderBy(F.col("burst_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

with_prev = filtered.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

profiled = (
    sessionized.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
        "t.last_event_at",
    )
)

metrics = (
    profiled
    .select(
        "*",
        F.sum("hours_opened_at_to_closed_at").over(w_prev_opened_at_to_date_event).alias("prev_opened_at_to_date_event_count_sum_hours_ope"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_prev_opened_at_to_date_event), 2).alias("requester_id_vs_prev_opened_at_to_date_event_cou"),
        F.lag("burst_id").over(w_new_session_flag_agent_id_by).alias("prev_burst_id"),
        F.sum("burst_id").over(w_new_session_flag_agent_id_by_2).alias("running_burst_id"),
    )
)

result = (
    metrics
    .groupingSets(
        [["agent_id", "channel", "priority"], ["agent_id"], []],
        "agent_id", "channel", "priority",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.round(F.stddev("prev_opened_at_to_date_event_count_sum_hours_ope"), 2).alias("stddev_prev_opened_at_to_date_event_count_sum_ho"),
        F.max("burst_id").alias("max_burst_id"),
        F.round(F.avg("running_burst_id"), 2).alias("avg_running_burst_id"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
