from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_agent_id_event_type_by_ticke = Window.partitionBy("agent_id", "event_type").orderBy("ticket_count", "id")

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

summarized_rows = (
    in_window.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "ticket_count",
        F.lead("event_at").over(w_event_type_ticket_id_by_even).alias("next_event_at"),
        F.lead("ticket_count", 2).over(w_event_type_ticket_id_by_even).alias("next2_ticket_count"),
        F.dense_rank().over(w_agent_id_event_type_by_ticke).alias("ticket_count_dense_rank"),
        (F.col("ticket_count") - F.lag("ticket_count").over(w_agent_id_event_type_by_ticke)).alias("ticket_count_change"),
        F.ntile(10).over(w_agent_id_event_type_by_ticke).alias("ticket_count_decile"),
    )
)

with_activity = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.array_contains(F.col("t.tags"), "login")), "left_anti")
    .filter(F.col("agent_id").isNotNull())
)

result = (
    with_activity
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "ticket_count",
        "next2_ticket_count",
        "ticket_count_dense_rank",
        "ticket_count_change",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
    .orderBy(F.col("event_type").desc())
)
