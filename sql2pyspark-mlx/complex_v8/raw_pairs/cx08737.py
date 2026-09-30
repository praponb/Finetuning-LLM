from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hours_since_open = Window.orderBy("hours_since_open", "id")
w_event_at_year = Window.partitionBy(F.year("event_at"))
w_all_by_hours_since_open_runn = Window.orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_count = Window.partitionBy("ticket_count")
w_ticket_count_by_ticket_count = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count").desc())
w_ticket_count_by_ticket_count_2 = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count").desc(), "id")

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

sequenced = (
    matched
    .select(
        "*",
        F.lag("event_at", 3).over(w_all_by_hours_since_open).alias("prev3_event_at"),
        F.size(F.collect_set("event_type").over(w_event_at_year)).alias("distinct_event_type_in_event_at_year"),
        F.lag("hours_since_open", 2).over(w_all_by_hours_since_open).alias("prev2_hours_since_open"),
        F.count(F.lit(1)).over(w_all_by_hours_since_open_runn).alias("running_count"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("requester_id").alias("min_ticket_requester_id"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    sequenced.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.min_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

with_windows = (
    with_stats
    .select(
        "*",
        F.round(F.avg("min_ticket_requester_id").over(w_ticket_count), 2).alias("ticket_count_avg_min_ticket_requester_id"),
        F.round(F.cume_dist().over(w_ticket_count_by_ticket_count), 3).alias("ticket_count_cume_dist"),
        F.count(F.lit(1)).over(w_ticket_count).alias("rows_in_ticket_count"),
        F.size(F.collect_set("event_type").over(w_ticket_count)).alias("distinct_event_type_in_ticket_count"),
        (F.col("prev2_hours_since_open") - F.lag("prev2_hours_since_open").over(w_ticket_count_by_ticket_count_2)).alias("prev2_hours_since_open_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "prev3_event_at",
        "distinct_event_type_in_event_at_year",
        "prev2_hours_since_open",
        "running_count",
        "min_ticket_requester_id",
        "ticket_count",
        "ticket_count_avg_min_ticket_requester_id",
        "ticket_count_cume_dist",
        "rows_in_ticket_count",
        "distinct_event_type_in_ticket_count",
        "prev2_hours_since_open_change",
    )
    .orderBy(
        F.col("running_count").desc(),
        F.col("ticket_count_cume_dist").desc_nulls_first(),
    )
)
