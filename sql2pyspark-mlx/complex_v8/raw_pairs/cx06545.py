from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_id_by_event_at_2 = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at")

scoped = (
    df_ticket_events
    .filter((F.col("event_type").isin("created", "assigned", "escalated")) & (F.col("actor").isin("customer", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_agent_id",
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("last_known_event_at"),
        F.first("event_at").over(w_ticket_id_event_type_by_even).alias("first_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("rn"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even_2).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at").over(w_ticket_id_by_event_at_2).alias("prev_event_at_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_event_type_by_even_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.rank().over(w_ticket_id_event_type_by_even_3).alias("event_at_rank"),
    )
)

first_rows = ranked.filter(F.col("rn") <= 2)

result = (
    first_rows
    .filter(F.col("minutes_since_prev_event_at") < 4000.0)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "last_known_event_at",
        "second_event_at",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "prev_event_at_2",
        "minutes_since_prev_event_at_2",
        "event_at_rank",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
    .orderBy("second_event_at", "event_at")
)
