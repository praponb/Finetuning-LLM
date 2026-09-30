from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_date_p = Window.partitionBy("ticket_id").orderBy(F.col("event_at_date").desc(), "id").rowsBetween(-6, -1)
w_ticket_id_by_event_at_date = Window.partitionBy("ticket_id").orderBy(F.col("event_at_date").desc(), "id")

base_events = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

windowed = (
    base_events
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_type_actor_label",
        "event_at_date",
        "event_at_quarter",
        F.round(F.avg("event_at_quarter").over(w_ticket_id_by_event_at_date_p), 2).alias("event_at_quarter_avg_6_prior"),
        F.dense_rank().over(w_ticket_id_by_event_at_date).alias("event_at_date_dense_rank"),
    )
)

with_parent_ticket = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
    )
)

without_matches = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") <= 3), "left_semi")
)

actor_stats = (
    without_matches
    .groupBy("actor")
    .agg(F.max("event_at_quarter").alias("max_event_at_quarter"))
)

outliers = (
    without_matches.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_quarter") == F.col("g.max_event_at_quarter"))
    .select(
        "s.*",
        "g.max_event_at_quarter",
        F.round(F.col("s.event_at_quarter") - F.col("g.max_event_at_quarter"), 2).alias("event_at_quarter_minus_max"),
    )
)

without_matches_2 = (
    outliers
    .crossJoin(outliers.agg(F.avg("ticket_requester_id").alias("overall_avg_ticket_requester_id")))
    .filter(F.col("ticket_requester_id") > F.col("overall_avg_ticket_requester_id"))
    .drop("overall_avg_ticket_requester_id")
)

combined_details_ticket = (
    without_matches_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel").isin("chat", "web", "email"))
    .select("s.*", F.col("t.channel").alias("ticket_channel"))
)

result = (
    combined_details_ticket
    .select("agent_team", "id", "ticket_tags")
    .distinct()
    .orderBy(F.col("agent_team").desc())
)
