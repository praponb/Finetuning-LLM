from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_priority_by_event = Window.partitionBy("ticket_id", "priority").orderBy("event_at")
w_event_type_priority = Window.partitionBy("event_type", "priority")
w_ticket_id_priority_by_event_2 = Window.partitionBy("ticket_id", "priority").orderBy("event_at", "id")

joined_ranges = (
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

with_trends = (
    joined_ranges
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        F.dense_rank().over(w_ticket_id_priority_by_event).alias("event_at_dense_rank"),
        F.count(F.lit(1)).over(w_event_type_priority).alias("rows_in_event_type_priority"),
        F.round(F.avg("hours_since_open").over(w_event_type_priority), 2).alias("event_type_priority_avg_hours_since_open"),
        F.size(F.collect_set("ticket_id").over(w_event_type_priority)).alias("distinct_ticket_id_in_event_type_priority"),
        F.round(F.cume_dist().over(w_ticket_id_priority_by_event_2), 3).alias("event_at_cume_dist"),
    )
)

exploded_detail = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-12").cast("date")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.rows_in_event_type_priority",
        "s.event_type_priority_avg_hours_since_open",
        "s.distinct_ticket_id_in_event_type_priority",
        "s.event_at_cume_dist",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.event_at").alias("event_event_at"),
    )
)

scored = (
    exploded_detail
    .filter(F.col("priority") == "P3")
    .select(
        "id",
        "ticket_id",
        "priority",
        "rows_in_event_type_priority",
        "event_type_priority_avg_hours_since_open",
        "event_id",
        "event_type",
        "event_event_at",
        F.coalesce(F.col("event_at_cume_dist"), F.lit(0.0)).alias("event_at_cume_dist_filled"),
        F.coalesce(F.col("event_at_cume_dist"), F.lit(0.0)).alias("event_at_cume_dist_filled_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "priority",
        "rows_in_event_type_priority",
        "event_type_priority_avg_hours_since_open",
        "event_type",
        "event_at_cume_dist_filled",
        F.date_format(F.col("event_event_at"), "MMM yyyy").alias("event_event_at_label"),
        F.concat(F.col("priority"), F.lit("#"), F.col("event_id").cast("string")).alias("priority_ref"),
    )
    .orderBy("event_type")
)
