from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "agent_id").orderBy(F.col("hours_since_open").desc(), F.col("id").desc())

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

deduped = (
    matched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

annotated = (
    deduped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.concat_ws(" / ", F.col("event_type"), F.col("priority")).alias("event_type_priority_label"),
    )
)

tagged = (
    annotated
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "event_type_priority_label",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.round(F.col("hours_since_open") * 1.5, 2).alias("hours_since_open_adj"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

result = (
    tagged
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "event_type_priority_label",
        "days_since_event_at",
        "hours_since_open_adj",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        (F.col("days_since_event_at") - F.col("event_at_hour")).alias("days_since_event_at_minus_event_at_hour"),
    )
)
