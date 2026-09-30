from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_priority_len = Window.orderBy(F.col("priority_len").desc_nulls_first(), "id")
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_all_by_priority_len_prior_2 = Window.orderBy(F.col("priority_len").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_agent_id = Window.partitionBy("agent_id")

valid = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
    )
)

enriched = (
    valid
    .select(
        "*",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "customer"), "left_semi")
)

metrics = (
    with_activity
    .select(
        "*",
        F.lag("priority_len", 1, 0).over(w_all_by_priority_len).alias("prev_priority_len"),
        F.max("priority_len").over(w_priority_agent_id).alias("priority_agent_id_max_priority_len"),
        F.max("priority_len").over(w_all_by_priority_len_prior_2).alias("priority_len_max_2_prior"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    metrics.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

with_windows = (
    summarized_rows
    .select(
        "*",
        F.round(F.col("priority_len") / F.nullif(F.sum("priority_len").over(w_agent_id), F.lit(0)), 4).alias("priority_len_share_of_agent_id"),
        F.round(F.col("prev_priority_len") - F.avg("prev_priority_len").over(w_agent_id), 2).alias("prev_priority_len_vs_agent_id_avg"),
        F.sum("prev_priority_len").over(w_agent_id).alias("agent_id_sum_prev_priority_len"),
    )
)

with_activity_2 = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") < 4) & (F.col("t.priority") == "P4"), "left_anti")
)

result = (
    with_activity_2
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "priority_len",
        "channel_priority_label",
        "priority_clean",
        "priority_agent_id_max_priority_len",
        "priority_len_max_2_prior",
        "last_event_at",
        "priority_len_share_of_agent_id",
        "prev_priority_len_vs_agent_id_avg",
        "agent_id_sum_prev_priority_len",
        (F.col("priority_len") + F.col("prev_priority_len")).alias("priority_len_plus_prev_priority_len"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
    .orderBy(F.col("channel_or_unknown").desc(), F.col("priority").desc())
)
