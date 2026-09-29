from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type").orderBy("ticket_requester_id", "id", "ticket_id_2")

matched = (
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

with_children_tickets = (
    matched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

deduped = (
    with_children_tickets
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "ticket_id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "ticket_id_2",
        "ticket_requester_id",
        "ticket_opened_at",
        F.round(F.col("hours_since_open") * 1.07, 2).alias("hours_since_open_adj"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
