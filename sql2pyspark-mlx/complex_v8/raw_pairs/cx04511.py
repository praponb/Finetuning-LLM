from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_event_at_2_month_by_ti = Window.partitionBy(F.date_trunc("month", "event_event_at_2")).orderBy(F.col("ticket_closed_at").desc())
w_event_type_ticket_id_by_tick = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("ticket_closed_at").desc())

valid = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_children = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

without_matches = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("agent", "system")), "left_anti")
)

enriched_event = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.col("t.event_type").isNotNull()), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "inner")
    .filter(F.col("t.event_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_id",
        "s.event_event_type",
        "s.event_event_at",
        F.col("t.event_at").alias("event_event_at_2"),
        F.col("ti.priority").alias("ticket_priority"),
        F.col("ti.closed_at").alias("ticket_closed_at"),
    )
)

sequenced = (
    enriched_event
    .select(
        "*",
        F.dense_rank().over(w_event_event_at_2_month_by_ti).alias("ticket_closed_at_dense_rank"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_tick), 3).alias("ticket_closed_at_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_id",
        "event_event_type",
        "event_event_at",
        "event_event_at_2",
        "ticket_priority",
        "ticket_closed_at",
        "ticket_closed_at_dense_rank",
        F.round((F.unix_timestamp(F.col("event_event_at")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_event_event_at"),
    )
    .orderBy(
        F.col("ticket_closed_at").asc_nulls_last(),
        F.col("ticket_priority").asc(),
    )
)
