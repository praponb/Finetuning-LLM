from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
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

ranked = (
    matched
    .select(
        "id",
        "ticket_id",
        "event_type",
        "priority",
        "hours_since_open",
        F.row_number().over(w_event_type_by_event_at).alias("rn"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.count(F.lit(1)).over(w_event_type_by_event_at_runni).alias("running_count"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count("*").alias("event_count_3"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        F.coalesce(F.col("t.event_count_3"), F.lit(0)).alias("event_count_3"),
    )
)

with_parent_ticket = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    with_parent_ticket
    .groupingSets(
        [["event_count", "rn"], ["event_count"], ["rn"], []],
        "event_count", "rn",
    )
    .agg(
        F.grouping("event_count").alias("is_total_event_count"),
        F.round(100 * F.count(F.when(F.col("ticket_priority") == "P3", 1)) / F.count("*"), 1).alias("pct_ticket_priority_p3"),
        F.sum("event_at_rank").alias("total_event_at_rank"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
    .orderBy(F.col("event_count").asc_nulls_last(), F.col("rn").asc_nulls_last())
)
