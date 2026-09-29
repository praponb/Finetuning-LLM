from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_plus_1 = Window.partitionBy("ticket_id").orderBy("event_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_plus_1_2 = Window.partitionBy("ticket_id").orderBy("event_at_plus_15m", "id")

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m_2"),
    )
)

with_details = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_plus_15m",
        "s.event_at_plus_15m_2",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.nth_value("event_at_plus_15m", 2).over(w_ticket_id_by_event_at_plus_1).alias("second_event_at_plus_15m"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.nth_value("event_at_plus_15m", 2).over(w_all_by_event_at_full).alias("second_event_at_plus_15m_2"),
        F.last("event_at_plus_15m_2").over(w_ticket_id_by_event_at_plus_1).alias("last_event_at_plus_15m_2"),
        F.ntile(3).over(w_ticket_id_by_event_at_plus_1_2).alias("event_at_plus_15m_tercile"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id")) & (F.col("t.requester_id").between(2, 20)), "left_anti")
)

ticket_priority_stats = (
    without_matches
    .groupBy("ticket_priority")
    .agg(F.percentile_approx("event_at_quartile", 0.5).alias("median_event_at_quartile"))
)

compared = (
    without_matches.alias("s")
    .join(ticket_priority_stats.alias("g"), F.col("g.ticket_priority") == F.col("s.ticket_priority"), "inner")
    .filter(F.col("s.event_at_quartile") >= F.col("g.median_event_at_quartile"))
    .select(
        "s.*",
        "g.median_event_at_quartile",
        F.round(F.col("s.event_at_quartile") - F.col("g.median_event_at_quartile"), 2).alias("event_at_quartile_minus_median"),
    )
)

result = (
    compared
    .filter(F.hour(F.col("last_event_at_plus_15m_2")).between(6, 12))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_plus_15m_2",
        "ticket_agent_id",
        "ticket_tags",
        "agent_name",
        "event_at_quartile",
        "second_event_at_plus_15m_2",
        "last_event_at_plus_15m_2",
        "event_at_plus_15m_tercile",
        "median_event_at_quartile",
        "event_at_quartile_minus_median",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy(F.col("event_at_plus_15m_tercile").desc())
)
