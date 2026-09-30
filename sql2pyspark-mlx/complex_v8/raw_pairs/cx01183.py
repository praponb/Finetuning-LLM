from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_ticket_priority = Window.partitionBy("agent_id").orderBy("ticket_priority_count")
w_agent_id_by_ticket_priority_2 = Window.partitionBy("agent_id").orderBy("ticket_priority_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority = Window.partitionBy("priority")

matched = (
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

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority").isin("P4", "P1", "P2"), 1)).alias("ticket_priority_count"),
        F.count("*").alias("ticket_count"),
    )
)

summarized_rows = (
    matched.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

filtered = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("ticket_count").between(2, 7))
)

windowed = (
    filtered
    .select(
        "id",
        "ticket_id",
        "hours_since_open",
        "ticket_priority_count",
        "ticket_count",
        F.round(F.percent_rank().over(w_agent_id_by_ticket_priority), 3).alias("ticket_priority_count_percent_rank"),
        F.nth_value("ticket_priority_count", 2).over(w_agent_id_by_ticket_priority_2).alias("second_ticket_priority_count"),
        F.round(F.col("ticket_count") / F.nullif(F.sum("ticket_count").over(w_priority), F.lit(0)), 4).alias("ticket_count_share_of_priority"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
    )
)

result = (
    windowed
    .filter(F.col("ticket_priority_count_percent_rank") < 0.7)
    .select(
        "id",
        "ticket_id",
        "hours_since_open",
        "ticket_priority_count",
        "ticket_count",
        "ticket_priority_count_percent_rank",
        "second_ticket_priority_count",
        "ticket_count_share_of_priority",
        "rows_in_priority",
    )
    .orderBy(F.col("rows_in_priority").desc())
)
