from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc())

filtered = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.last("event_at", ignorenulls=True).over(w_event_type_ticket_id_by_even).alias("last_known_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_type_ticket_id_by_even).alias("first_known_event_at"),
        F.rank().over(w_event_type_ticket_id_by_even_2).alias("event_at_rank"),
    )
)

joined_ticket = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.last_known_event_at",
        F.col("t.agent_id").alias("ticket_agent_id"),
        "su.agent_name",
    )
)

tagged = (
    joined_ticket
    .filter(F.month(F.col("last_known_event_at")).isin(1, 3))
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_agent_id",
        "agent_name",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

derived = (
    tagged
    .select(
        "*",
        F.add_months(F.col("event_at_date"), 12).alias("event_at_date_plus_12m"),
    )
)

result = (
    derived
    .groupingSets(
        [["event_type", "ticket_agent_id", "actor"], ["event_type"], ["ticket_agent_id"], []],
        "event_type", "ticket_agent_id", "actor",
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum("days_since_event_at").alias("total_days_since_event_at"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_agent_id",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "actor_list",
        "total_days_since_event_at",
    )
)
