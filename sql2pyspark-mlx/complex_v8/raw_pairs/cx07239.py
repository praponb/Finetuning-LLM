from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_by_ticket_c = Window.partitionBy(F.to_date("event_at")).orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 19) & (F.col("actor") != "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

with_details = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

enriched = (
    with_details.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.ticket_agent_id",
        "s.ticket_closed_at",
        "s.ticket_priority",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_windows = (
    enriched
    .withColumn("first_known_ticket_closed_at", F.first("ticket_closed_at", ignorenulls=True).over(w_event_at_to_date_by_ticket_c))
)

grouped = (
    with_windows
    .groupBy("event_type")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("ticket_priority")).alias("ticket_priority_list"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.sum(F.when(F.col("ticket_priority") == "P3", 1).otherwise(0)).alias("n_ticket_priority_p3"),
    )
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.min("n_ticket_priority_p3").alias("overall_min_n_ticket_priority_p3")))
    .filter(F.col("n_ticket_priority_p3") > F.col("overall_min_n_ticket_priority_p3"))
    .drop("overall_min_n_ticket_priority_p3")
)

result = (
    with_activity
    .select(
        "event_type",
        "last_event_at",
        "distinct_ids",
        "ticket_priority_list",
        "agent_name_list",
        F.round(F.col("distinct_ids") / F.nullif(F.col("n_ticket_priority_p3"), F.lit(0)), 3).alias("distinct_ids_per_n_ticket_priority_p3"),
        F.date_trunc("week", F.col("last_event_at")).alias("last_event_at_week"),
    )
    .orderBy(F.col("last_event_at").desc())
)
