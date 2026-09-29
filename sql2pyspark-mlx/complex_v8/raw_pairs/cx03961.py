from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("agent_id") >= 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

priority_stats = (
    enriched
    .groupBy("priority")
    .agg(F.avg("greatest_requester_id_csat").alias("avg_greatest_requester_id_csat"))
)

above_average = (
    enriched.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.greatest_requester_id_csat") > F.col("g.avg_greatest_requester_id_csat"))
    .select(
        "s.*",
        "g.avg_greatest_requester_id_csat",
        F.round(F.col("s.greatest_requester_id_csat") - F.col("g.avg_greatest_requester_id_csat"), 2).alias("greatest_requester_id_csat_minus_avg"),
    )
)

per_label = (
    above_average
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "days_since_closed_at",
        "greatest_requester_id_csat",
        "opened_at_month",
        "agent_team",
        "agent_name",
        "agent_hired_on",
        "avg_greatest_requester_id_csat",
        "greatest_requester_id_csat_minus_avg",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    per_label.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

result = (
    with_stats
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "days_since_closed_at",
        "opened_at_month",
        "agent_hired_on",
        "tag_pos",
        "tag",
        "last_event_at",
        F.round((F.unix_timestamp(F.col("last_event_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_last_event_at"),
    )
    .orderBy(F.col("csat").desc(), F.col("id").asc(), "tag_pos")
    .limit(5)
)
