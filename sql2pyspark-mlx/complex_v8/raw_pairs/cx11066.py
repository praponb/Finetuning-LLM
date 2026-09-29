from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_count_tier_2_by_opene = Window.partitionBy("ticket_count_tier_2").orderBy("opened_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_count_tier_2_by_opene_2 = Window.partitionBy("ticket_count_tier_2").orderBy("opened_at", "id", "event_id")
w_all_by_opened_at = Window.orderBy("opened_at", "id", "event_id")
w_all_by_opened_at_seq = Window.orderBy("opened_at_seq", "id", "event_id")
w_ticket_count_tier_2_event_ac = Window.partitionBy("ticket_count_tier_2", "event_actor").orderBy("last_known_event_at", "id", "event_id")
w_prev_opened_at_month_priorit = Window.partitionBy(F.month("prev_opened_at"), "priority_group")

scoped = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

profiled = (
    scoped.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

filtered = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

scored = (
    filtered
    .filter(F.col("opened_at") >= F.lit("2025-01-07 13:00:00").cast("timestamp"))
    .select(
        "id",
        "opened_at",
        "priority",
        F.when(F.col("priority").isin("P3", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.when(F.col("ticket_count") >= 6, "large").when(F.col("ticket_count") >= 2, "medium").otherwise("small").alias("ticket_count_tier"),
        F.when(F.col("ticket_count") >= 4, "HIGH").when(F.col("ticket_count") >= 2, "MEDIUM").otherwise("LOW").alias("ticket_count_tier_2"),
    )
)

with_children = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.opened_at",
        "s.priority",
        "s.priority_group",
        "s.ticket_count_tier_2",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

ranked = (
    with_children
    .select(
        "id",
        "opened_at",
        "priority_group",
        "ticket_count_tier_2",
        "event_id",
        "event_actor",
        "event_at",
        F.last("event_at", ignorenulls=True).over(w_ticket_count_tier_2_by_opene).alias("last_known_event_at"),
        F.row_number().over(w_ticket_count_tier_2_by_opene_2).alias("opened_at_seq"),
        F.lag("opened_at").over(w_ticket_count_tier_2_by_opene_2).alias("prev_opened_at"),
        F.lag("opened_at").over(w_all_by_opened_at).alias("prev_opened_at_2"),
        F.round(F.percent_rank().over(w_ticket_count_tier_2_by_opene_2), 3).alias("opened_at_percent_rank"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round((F.unix_timestamp("last_known_event_at") - F.unix_timestamp(F.lag("last_known_event_at").over(w_all_by_opened_at_seq))) / 60, 1).alias("minutes_since_prev_last_known_event_at"),
        F.round(F.percent_rank().over(w_ticket_count_tier_2_event_ac), 3).alias("last_known_event_at_percent_rank"),
        F.ntile(3).over(w_ticket_count_tier_2_event_ac).alias("last_known_event_at_tercile"),
        F.count(F.lit(1)).over(w_prev_opened_at_month_priorit).alias("rows_in_prev_opened_at_month_priority_group"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_id",
        F.col("opened_at_percent_rank").alias("m_opened_at_percent_rank"),
        F.col("last_known_event_at_percent_rank").alias("m_last_known_event_at_percent_rank"),
        F.col("minutes_since_prev_last_known_event_at").alias("minutes_since_prev_last_known_event_at_v"),
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["m_opened_at_percent_rank", "m_last_known_event_at_percent_rank", "minutes_since_prev_last_known_event_at_v"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
