from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_opened_at_month_by_ev = Window.partitionBy(F.date_trunc("month", "ticket_opened_at")).orderBy("event_actor_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_opened_at_month_by_ev_2 = Window.partitionBy(F.date_trunc("month", "ticket_opened_at")).orderBy("event_actor_count", "id").rowsBetween(-2, Window.currentRow)
w_ticket_id_by_running_event_a = Window.partitionBy("ticket_id").orderBy("running_event_actor_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor = Window.partitionBy("actor")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_actor_count_by_running = Window.partitionBy("event_actor_count").orderBy(F.col("running_event_actor_count").asc_nulls_last(), "id")
w_event_type_actor = Window.partitionBy("event_type", "actor")

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(~F.col("actor").isin("agent", "system"), 1)).alias("event_actor_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    base_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.first_event_at",
    )
)

with_details = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.opened_at") < F.lit("2025-02-18 07:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_actor_count",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.sum("event_actor_count").over(w_ticket_opened_at_month_by_ev).alias("running_event_actor_count"),
        F.round(F.avg("event_actor_count").over(w_ticket_opened_at_month_by_ev_2), 2).alias("event_actor_count_avg_3"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.nth_value("ticket_opened_at", 2).over(w_ticket_id_by_running_event_a).alias("second_ticket_opened_at"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.count(F.lit(1)).over(w_actor_event_type_by_event_at).alias("running_count"),
        F.last("ticket_closed_at").over(w_ticket_id_by_running_event_a).alias("last_ticket_closed_at"),
        F.sum("event_actor_count").over(w_actor_event_type_by_event_at).alias("running_event_actor_count_2"),
    )
)

windowed = (
    ranked_2
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_actor_count",
        "ticket_closed_at",
        "ticket_opened_at",
        "running_event_actor_count",
        "rows_in_actor",
        "running_count",
        "last_ticket_closed_at",
        "running_event_actor_count_2",
        F.lag("last_ticket_closed_at", 3).over(w_event_actor_count_by_running).alias("prev3_last_ticket_closed_at"),
        F.round(F.avg("rows_in_actor").over(w_event_type_actor), 2).alias("event_type_actor_avg_rows_in_actor"),
    )
)

result = (
    windowed
    .select("rows_in_actor", "id", "actor")
    .distinct()
    .orderBy("id")
)
