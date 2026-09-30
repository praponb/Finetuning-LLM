from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_actor = Window.partitionBy("actor")

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") != "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

windowed = (
    base_events
    .select(
        "*",
        F.dense_rank().over(w_event_type_by_event_at_ties).alias("event_at_dense_rank"),
        F.size(F.collect_set("ticket_id").over(w_actor)).alias("distinct_ticket_id_in_actor"),
        F.sum("days_since_event_at").over(w_actor).alias("actor_sum_days_since_event_at"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("replied", "escalated", "created")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-08").cast("date")), "left_anti")
    .filter(F.col("ticket_id") >= 4)
)

exploded_detail_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-08").cast("date")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

enriched_ticket = (
    exploded_detail_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.days_since_event_at",
        "s.event_id",
        "s.event_event_at",
        "s.event_event_type",
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    enriched_ticket
    .groupingSets(
        [["ticket_priority", "event_event_type"], ["ticket_priority"], []],
        "ticket_priority", "event_event_type",
    )
    .agg(
        F.grouping("ticket_priority").alias("is_total_ticket_priority"),
        F.count("*").alias("cnt"),
        F.max("days_since_event_at").alias("max_days_since_event_at"),
        F.round(F.avg("days_since_event_at"), 2).alias("avg_days_since_event_at"),
    )
    .orderBy(
        F.col("ticket_priority").asc_nulls_last(),
        F.col("event_event_type").asc_nulls_last(),
    )
)
