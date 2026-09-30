from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_prior_1 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-1, -1)
w_priority_ticket_id = Window.partitionBy("priority", "ticket_id")
w_all_by_event_at = Window.orderBy("event_at", "id")

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

without_matches = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") == "T3") & (~F.col("su.team").isin("VIP", "Tech")), "left_semi")
)

enriched_agent = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

with_trends = (
    enriched_agent
    .select(
        "*",
        F.round(F.avg("hours_since_open").over(w_all_by_event_at_prior_1), 2).alias("hours_since_open_avg_1_prior"),
        F.min("hours_since_open").over(w_priority_ticket_id).alias("priority_ticket_id_min_hours_since_open"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.round(F.col("hours_since_open") - F.avg("hours_since_open").over(w_priority_ticket_id), 2).alias("hours_since_open_vs_priority_ticket_id_avg"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["event_type", "agent_id", "ticket_id"], ["event_type"], []],
        "event_type", "agent_id", "ticket_id",
    )
    .agg(
        F.sum(F.when(F.col("priority") == "P1", 1).otherwise(0)).alias("n_priority_p1"),
        F.round(100 * F.count(F.when(F.col("priority").isin("P3", "P4"), 1)) / F.count("*"), 1).alias("pct_priority_in_p3_p4"),
    )
)
