from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_row_count_full = Window.orderBy("row_count", "event_id", "event_actor", "event_count").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_actor_row_count_by_tic = Window.partitionBy("event_actor", "row_count").orderBy(F.col("ticket_requester_id_event_actor_customer").desc(), "event_id", "event_actor", "event_count").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 13)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-01-16 00:00:00").cast("timestamp")), "inner")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

line_level_events = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_plus_1d",
        "s.ticket_requester_id",
        "s.ticket_agent_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

profiled = (
    line_level_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

event_actor_stats = (
    profiled
    .groupBy("event_actor")
    .agg(F.percentile_approx("ticket_requester_id", 0.5).alias("median_ticket_requester_id"))
)

vs_group = (
    profiled.alias("s")
    .join(event_actor_stats.alias("g"), F.col("g.event_actor") == F.col("s.event_actor"), "inner")
    .filter(F.col("s.ticket_requester_id") >= F.col("g.median_ticket_requester_id"))
    .select(
        "s.*",
        "g.median_ticket_requester_id",
        F.round(F.col("s.ticket_requester_id") - F.col("g.median_ticket_requester_id"), 2).alias("ticket_requester_id_minus_median"),
    )
)

summary = (
    vs_group
    .groupBy("event_id", "event_actor", "event_count")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("ticket_requester_id").alias("total_ticket_requester_id"),
        F.sum(F.when(F.col("event_actor") != "customer", F.col("ticket_requester_id"))).alias("ticket_requester_id_event_actor_customer"),
        F.sum("median_ticket_requester_id").alias("total_median_ticket_requester_id"),
        F.sum(F.when(F.col("event_type") == "escalated", F.col("event_count"))).alias("event_count_event_type_escalated"),
    )
)

ranked = (
    summary
    .select(
        "event_id",
        "event_actor",
        "event_count",
        "total_ticket_requester_id",
        "ticket_requester_id_event_actor_customer",
        "total_median_ticket_requester_id",
        F.nth_value("total_ticket_requester_id", 2).over(w_all_by_row_count_full).alias("second_total_ticket_requester_id"),
        F.first("row_count", ignorenulls=True).over(w_event_actor_row_count_by_tic).alias("first_known_row_count"),
    )
)

result = (
    ranked
    .select(
        "event_id",
        "event_actor",
        "total_ticket_requester_id",
        "ticket_requester_id_event_actor_customer",
        "total_median_ticket_requester_id",
        "second_total_ticket_requester_id",
        (F.col("event_count") + F.col("first_known_row_count")).alias("event_count_plus_first_known_row_count"),
    )
    .orderBy("event_id", F.col("total_median_ticket_requester_id").desc())
)
