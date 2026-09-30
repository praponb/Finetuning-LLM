from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

joined_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_year",
        "s.actor_clean",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

actor_stats = (
    joined_ticket
    .groupBy("actor")
    .agg(F.avg("event_at_year").alias("avg_event_at_year"))
)

outliers = (
    joined_ticket.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_year") > F.col("g.avg_event_at_year"))
    .select(
        "s.*",
        "g.avg_event_at_year",
        F.round(F.col("s.event_at_year") - F.col("g.avg_event_at_year"), 2).alias("event_at_year_minus_avg"),
    )
)

event_type_stats = (
    outliers
    .groupBy("event_type")
    .agg(F.avg("avg_event_at_year").alias("avg_avg_event_at_year"))
)

above_average = (
    outliers.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.avg_event_at_year") > F.col("g.avg_avg_event_at_year"))
    .select(
        "s.*",
        "g.avg_avg_event_at_year",
        F.round(F.col("s.avg_event_at_year") - F.col("g.avg_avg_event_at_year"), 2).alias("avg_event_at_year_minus_avg"),
    )
)

exploded_detail = (
    above_average.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_year",
        "s.actor_clean",
        "s.ticket_closed_at",
        "s.ticket_priority",
        "s.ticket_opened_at",
        "s.avg_event_at_year",
        "s.event_at_year_minus_avg",
        "s.avg_avg_event_at_year",
        "s.avg_event_at_year_minus_avg",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["actor", "event_type", "event_id"], ["actor"]],
        "actor", "event_type", "event_id",
    )
    .agg(
        F.grouping("actor").alias("is_total_actor"),
        F.sum(F.when(F.col("ticket_priority") == "P1", F.col("avg_avg_event_at_year")).otherwise(0)).alias("avg_avg_event_at_year_ticket_priority_p1"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_id",
        "is_total_actor",
        "avg_avg_event_at_year_ticket_priority_p1",
    )
)
