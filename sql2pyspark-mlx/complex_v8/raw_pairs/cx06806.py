from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")

prepared = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("ticket_id") <= 58))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.lag("event_at", 3).over(w_ticket_id_by_event_at).alias("prev3_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
    )
)

ticket_id_stats = (
    with_windows
    .groupBy("ticket_id")
    .agg(F.percentile_approx("minutes_since_prev_event_at", 0.5).alias("median_minutes_since_prev_event_at"))
)

vs_group = (
    with_windows.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.minutes_since_prev_event_at") >= F.col("g.median_minutes_since_prev_event_at"))
    .select(
        "s.*",
        "g.median_minutes_since_prev_event_at",
        F.round(F.col("s.minutes_since_prev_event_at") - F.col("g.median_minutes_since_prev_event_at"), 2).alias("minutes_since_prev_event_at_minus_median"),
    )
)

enriched_ticket = (
    vs_group.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.second_event_at",
        "s.prev3_event_at",
        "s.event_at_percent_rank",
        "s.minutes_since_prev_event_at",
        "s.prev_event_at",
        "s.median_minutes_since_prev_event_at",
        "s.minutes_since_prev_event_at_minus_median",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

joined = (
    enriched_ticket.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    joined
    .filter(F.col("event_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "second_event_at",
        "minutes_since_prev_event_at",
        "prev_event_at",
        "median_minutes_since_prev_event_at",
        "minutes_since_prev_event_at_minus_median",
        "ticket_requester_id",
        "ticket_agent_id",
        "ticket_priority",
        "agent_name",
        "agent_hired_on",
        "agent_team",
        F.abs(F.col("event_at_percent_rank") - 0.7).alias("event_at_percent_rank_distance"),
    )
    .orderBy(F.col("agent_hired_on").asc())
)
