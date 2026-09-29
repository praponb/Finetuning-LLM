from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_by_event_at_t = Window.partitionBy("ticket_channel").orderBy("event_at")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_channel_by_event_at = Window.partitionBy("ticket_channel").orderBy("event_at", "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

with_details = (
    joined_ranges.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.agent_id",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("t.channel").alias("ticket_channel"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.rank().over(w_ticket_channel_by_event_at_t).alias("event_at_rank"),
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_channel_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.row_number().over(w_ticket_channel_by_event_at).alias("seq_no"),
    )
)

result = (
    ranked
    .filter(F.to_date(F.col("next_event_at")) < F.lit("2025-01-31").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "agent_id",
        "agent_tier",
        "ticket_channel",
        "event_at_rank",
        "next_event_at",
        "minutes_since_prev_event_at",
        F.round(F.col("event_at_rank") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("event_at_rank_per_seq_no"),
        F.coalesce(F.col("ticket_channel"), F.lit("UNKNOWN")).alias("ticket_channel_or_unknown"),
    )
    .orderBy(F.col("agent_tier").desc_nulls_first(), "id")
    .limit(10)
)
