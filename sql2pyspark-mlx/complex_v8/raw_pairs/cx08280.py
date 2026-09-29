from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_agent_hired_on_mo = Window.partitionBy("event_type", F.month("agent_hired_on")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_or_unknown_by_event_at = Window.partitionBy("actor_or_unknown").orderBy("event_at", "id")
w_event_type_group = Window.partitionBy("event_type_group")

recent_events = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_type") == "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.when(F.col("event_type").isin("created", "escalated"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
    )
)

scored = (
    recent_events
    .select(
        "*",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref_2"),
    )
)

enriched_ticket = (
    scored.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.opened_at")).between(7, 11)), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        "s.event_type_group",
        "s.actor_or_unknown",
        "s.event_type_ref",
        "s.event_type_ref_2",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_event_type_agent_hired_on_mo).alias("last_known_event_at"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_actor_or_unknown_by_event_at)).alias("days_since_prev_agent_hired_on"),
        F.count(F.lit(1)).over(w_event_type_group).alias("rows_in_event_type_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    metrics.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    summarized_rows
    .select("id")
    .distinct()
    .orderBy(F.col("id").desc_nulls_first())
)
