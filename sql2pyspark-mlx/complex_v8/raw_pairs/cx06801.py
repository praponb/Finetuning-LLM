from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id = Window.partitionBy("event_id")
w_ticket_agent_id_by_ticket_re = Window.partitionBy("ticket_agent_id").orderBy("ticket_requester_id", "id", "ticket_id_2", "event_id")
w_dedup = Window.partitionBy("ticket_agent_id", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc(), F.col("ticket_id_2").desc(), F.col("event_id").desc())

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

enriched = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.month(F.col("t.closed_at")).isin(2, 3)), "inner")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

exploded_detail_tickets = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_agent_id",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

exploded_detail = (
    exploded_detail_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_agent_id",
        "s.ticket_id_2",
        "s.ticket_tags",
        "s.ticket_channel",
        "s.ticket_requester_id",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.event_at").alias("event_event_at"),
    )
)

windowed = (
    exploded_detail
    .select(
        "*",
        F.round(F.avg("ticket_requester_id").over(w_event_id), 2).alias("event_id_avg_ticket_requester_id"),
        F.sum("ticket_requester_id").over(w_event_id).alias("event_id_sum_ticket_requester_id"),
        F.row_number().over(w_ticket_agent_id_by_ticket_re).alias("ticket_requester_id_seq"),
    )
)

unique_rows = (
    windowed
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupBy("event_id")
    .pivot("event_type", ["escalated", "assigned", "replied", "reopened"])
    .agg(F.sum("ticket_requester_id_seq").alias("total"), F.count("ticket_requester_id_seq").alias("n"))
)
