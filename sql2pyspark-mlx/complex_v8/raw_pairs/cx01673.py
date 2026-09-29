from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_hours_since_open = Window.partitionBy("agent_id").orderBy(F.col("hours_since_open").desc(), "id")
w_priority_len = Window.partitionBy("priority_len")
w_agent_id_by_hours_since_open_2 = Window.partitionBy("agent_id").orderBy(F.col("hours_since_open").desc())

covered = (
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

enriched = (
    covered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "agent_id",
        "hours_since_open",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "event_at",
        "agent_id",
        "hours_since_open",
        "priority_len",
        F.lag("hours_since_open", 3).over(w_agent_id_by_hours_since_open).alias("prev3_hours_since_open"),
        F.round(F.col("hours_since_open") - F.avg("hours_since_open").over(w_priority_len), 2).alias("hours_since_open_vs_priority_len_avg"),
        F.round(F.percent_rank().over(w_agent_id_by_hours_since_open_2), 3).alias("hours_since_open_percent_rank"),
        F.max("hours_since_open").over(w_priority_len).alias("priority_len_max_hours_since_open"),
    )
)

exploded_detail_tickets = (
    metrics.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.hours_since_open",
        "s.priority_len",
        "s.prev3_hours_since_open",
        "s.hours_since_open_vs_priority_len_avg",
        "s.hours_since_open_percent_rank",
        F.col("t.id").alias("ticket_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    exploded_detail_tickets
    .groupingSets(
        [["ticket_channel", "agent_id"], ["ticket_channel"], ["agent_id"]],
        "ticket_channel", "agent_id",
    )
    .agg(
        F.percentile_approx("hours_since_open_vs_priority_len_avg", 0.5).alias("median_hours_since_open_vs_priority_len_avg"),
        F.round(F.avg("hours_since_open_vs_priority_len_avg"), 2).alias("avg_hours_since_open_vs_priority_len_avg"),
    )
    .select(
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "agent_id",
        "median_hours_since_open_vs_priority_len_avg",
        "avg_hours_since_open_vs_priority_len_avg",
    )
)
