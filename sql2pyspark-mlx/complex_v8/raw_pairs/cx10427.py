from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor").orderBy(F.col("event_at_hour").desc(), F.col("id").desc())

filtered_events = (
    df_ticket_events
    .filter((F.col("actor") != "agent") | (F.month(F.col("event_at")).isin(2, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

with_details = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

unique_rows = (
    with_details
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

tagged = unique_rows.withColumn("event_at_hour_filled", F.coalesce(F.col("event_at_hour"), F.lit(0)))

derived = (
    tagged
    .filter(F.col("event_at_hour") < 12)
    .select("*", F.year(F.col("ticket_opened_at")).alias("ticket_opened_at_year"))
)

combined_details_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id").between(6, 17))
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_at_hour",
        "s.ticket_opened_at",
        "s.ticket_opened_at_year",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

joined_ticket = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "actor",
        "ticket_opened_at",
        "ticket_opened_at_year",
        "ticket_csat",
        "ticket_closed_at",
        "agent_hired_on",
        F.abs(F.col("ticket_csat") - 3).alias("ticket_csat_distance"),
        F.concat_ws("|", F.col("actor"), F.col("agent_tier")).alias("actor_agent_tier_label"),
    )
    .orderBy(F.col("actor").desc(), F.col("actor_agent_tier_label").desc())
)
