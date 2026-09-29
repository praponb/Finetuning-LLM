from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_team").orderBy(F.col("event_at_year").desc(), F.col("id").desc(), F.col("event_id").desc())

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_parent_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(~F.col("t.priority").isin("P4", "P3"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_year",
        "s.event_type_slug",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

detail_rows_events = (
    with_parent_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_at_year",
        "s.event_type_slug",
        "s.ticket_priority",
        "s.ticket_channel",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

filtered = (
    detail_rows_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("ticket_id") <= 47)
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at_year",
        "s.ticket_priority",
        "s.ticket_channel",
        "s.event_id",
        "s.event_event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "event_at_year",
        "ticket_priority",
        "ticket_channel",
        "event_id",
        "event_event_at",
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("ticket_id").cast("string")).alias("ticket_channel_ref"),
        F.coalesce(F.col("ticket_priority"), F.lit("UNKNOWN")).alias("ticket_priority_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_at_year",
        "event_id",
        "event_event_at",
        "ticket_channel_ref",
        "ticket_priority_or_unknown",
        F.coalesce(F.col("event_at_year"), F.lit(0)).alias("event_at_year_filled"),
        F.when(F.col("ticket_channel").isin("chat", "phone"), "group_a").when(F.col("ticket_channel") == "email", "group_b").otherwise("other").alias("ticket_channel_group"),
    )
    .orderBy(F.col("event_at_year_filled").asc())
)
