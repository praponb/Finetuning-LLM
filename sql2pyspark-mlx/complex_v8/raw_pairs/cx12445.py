from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_first_ticket_closed_a = Window.orderBy(F.col("first_ticket_closed_at").desc(), "id")
w_all_by_first_ticket_closed_a_2 = Window.orderBy(F.col("first_ticket_closed_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_priority_event_at_month = Window.partitionBy("priority", F.date_trunc("month", "event_at"))

joined_ranges = (
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
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.max("opened_at").alias("last_ticket_opened_at"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_closed_at", "t.last_ticket_opened_at")
)

metrics = (
    profiled
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_first_ticket_closed_a), 3).alias("first_ticket_closed_at_cume_dist"),
        F.dense_rank().over(w_all_by_first_ticket_closed_a_2).alias("first_ticket_closed_at_dense_rank"),
        F.round((F.unix_timestamp("last_ticket_opened_at") - F.unix_timestamp(F.lag("last_ticket_opened_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_last_ticket_opened_at"),
        F.count(F.lit(1)).over(w_priority_event_at_month).alias("rows_in_priority_event_at_month"),
    )
)

ticket_id_stats = (
    metrics
    .groupBy("ticket_id")
    .agg(F.max("minutes_since_prev_last_ticket_opened_at").alias("max_minutes_since_prev_last_ticket_opened_at"))
)

above_average = (
    metrics.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.minutes_since_prev_last_ticket_opened_at") == F.col("g.max_minutes_since_prev_last_ticket_opened_at"))
    .select(
        "s.*",
        "g.max_minutes_since_prev_last_ticket_opened_at",
        F.round(F.col("s.minutes_since_prev_last_ticket_opened_at") - F.col("g.max_minutes_since_prev_last_ticket_opened_at"), 2).alias("minutes_since_prev_last_ticket_opened_at_minus_m"),
    )
)

result = (
    above_average
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "hours_since_open",
        "first_ticket_closed_at",
        "first_ticket_closed_at_cume_dist",
        "first_ticket_closed_at_dense_rank",
        "minutes_since_prev_last_ticket_opened_at",
        "rows_in_priority_event_at_month",
        "max_minutes_since_prev_last_ticket_opened_at",
        "minutes_since_prev_last_ticket_opened_at_minus_m",
        F.concat_ws(" / ", F.col("event_type"), F.col("priority")).alias("event_type_priority_label"),
    )
)
