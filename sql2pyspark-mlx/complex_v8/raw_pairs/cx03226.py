from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy(F.col("agent_hired_on").desc(), F.col("id").desc())

valid_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 3)) & (F.col("event_type").isin("escalated", "resolved")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

screened = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("t.actor") == "system"), "left_semi")
)

latest_per_key = (
    screened
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupBy("ticket_id")
    .pivot("event_type", ["escalated", "replied"])
    .agg(F.max("ticket_requester_id"))
)
