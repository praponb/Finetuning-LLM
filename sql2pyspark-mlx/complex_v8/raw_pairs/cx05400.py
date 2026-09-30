from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_all_by_hours_since_open = Window.orderBy("hours_since_open", "id")
w_all_by_hours_since_open_movi = Window.orderBy("hours_since_open", "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_by_event_at_prior = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(-1, -1)

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
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

with_windows = (
    in_window
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_ticket_id_event_type)).alias("distinct_agent_id_in_ticket_id_event_type"),
        F.lag("event_at").over(w_all_by_hours_since_open).alias("prev_event_at"),
        F.sum("hours_since_open").over(w_all_by_hours_since_open_movi).alias("hours_since_open_sum_7"),
        F.sum("hours_since_open").over(w_ticket_id_by_event_at_prior).alias("hours_since_open_sum_1_prior"),
        F.round(F.col("hours_since_open") - F.avg("hours_since_open").over(w_ticket_id_event_type), 2).alias("hours_since_open_vs_ticket_id_event_type_avg"),
        F.size(F.collect_set("priority").over(w_ticket_id_event_type)).alias("distinct_priority_in_ticket_id_event_type"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T2", "T1")) & (F.length(F.trim(F.col("su.agent_name"))) >= 5), "left_semi")
)

joined = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        "s.prev_event_at",
        "s.hours_since_open_sum_7",
        "s.hours_since_open_sum_1_prior",
        "s.hours_since_open_vs_ticket_id_event_type_avg",
        "s.distinct_priority_in_ticket_id_event_type",
        "su.agent_name",
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    joined
    .select(
        "id",
        "ticket_id",
        "hours_since_open",
        "prev_event_at",
        "hours_since_open_sum_7",
        "hours_since_open_vs_ticket_id_event_type_avg",
        "ticket_channel",
        F.least(F.col("hours_since_open"), F.col("hours_since_open_sum_7")).alias("least_hours_since_open_hours_since_open_sum_7"),
        F.hour(F.col("prev_event_at")).alias("prev_event_at_hour"),
    )
    .orderBy(F.col("ticket_channel").desc_nulls_first())
)
