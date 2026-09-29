from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_channel = Window.partitionBy("actor", "ticket_channel")
w_all_by_last_event_at_plus_7d = Window.orderBy(F.col("last_event_at_plus_7d").desc(), "id").rowsBetween(-3, -1)

valid_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(~F.col("t.channel").isin("phone", "email"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    enriched_ticket.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

labeled = (
    profiled
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel")), " ", "_")).alias("ticket_channel_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        (F.col("last_event_at") + F.expr("INTERVAL 7 DAYS")).alias("last_event_at_plus_7d"),
    )
)

scored = (
    labeled
    .filter(F.col("last_event_at_plus_7d") >= F.lit("2025-01-21 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_channel",
        "last_event_at",
        "last_event_at_2",
        "ticket_channel_slug",
        "event_type_slug",
        "last_event_at_plus_7d",
        F.length(F.trim(F.col("ticket_channel"))).alias("ticket_channel_len"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_csat").cast("string")).alias("actor_ref"),
        F.when(F.col("ticket_csat") >= 4, "HIGH").when(F.col("ticket_csat") >= 3, "MEDIUM").otherwise("LOW").alias("ticket_csat_tier"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.round(F.col("ticket_channel_len") / F.nullif(F.sum("ticket_channel_len").over(w_actor_ticket_channel), F.lit(0)), 4).alias("ticket_channel_len_share_of_actor_ticket_channel"),
        F.max("ticket_channel_len").over(w_all_by_last_event_at_plus_7d).alias("ticket_channel_len_max_3_prior"),
        F.sum("ticket_channel_len").over(w_all_by_last_event_at_plus_7d).alias("ticket_channel_len_sum_3_prior"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "ticket_requester_id",
        "ticket_channel_len_max_3_prior",
        "ticket_channel_len_sum_3_prior",
    )
    .unpivot(
        ids=["id"],
        values=["ticket_requester_id", "ticket_channel_len_max_3_prior", "ticket_channel_len_sum_3_prior"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
