from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_by_event_at = Window.partitionBy("ticket_priority").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_priority_by_event_at_2 = Window.partitionBy("ticket_priority").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_by_event_at_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")

recent_events = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

with_details_ticket = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

windowed = (
    with_details_ticket
    .select(
        "*",
        F.last("event_at").over(w_ticket_priority_by_event_at).alias("last_event_at"),
        F.ntile(10).over(w_ticket_priority_by_event_at_2).alias("event_at_decile"),
        F.last("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("last_known_event_at"),
        F.lag("event_at").over(w_ticket_priority_by_event_at_2).alias("prev_event_at"),
        F.lag("event_at", 3).over(w_ticket_priority_by_event_at_2).alias("prev3_event_at"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.first("event_at").over(w_ticket_priority_by_event_at).alias("first_event_at"),
        F.round(F.percent_rank().over(w_actor_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_actor_by_event_at).alias("next_event_at_2"),
        F.row_number().over(w_ticket_priority_by_event_at_2).alias("rn"),
    )
)

result = (
    windowed
    .filter(F.col("ticket_priority") == "P4")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_ref",
        "ticket_tags",
        "ticket_priority",
        "ticket_channel",
        "last_event_at",
        "event_at_decile",
        "last_known_event_at",
        "prev_event_at",
        "prev3_event_at",
        "second_event_at",
        "event_at_rank",
        "first_event_at",
        "event_at_percent_rank",
        "next_event_at_2",
        "rn",
        F.when(F.col("ticket_priority").isin("P1", "P2"), "group_a").when(F.col("ticket_priority") == "P4", "group_b").otherwise("other").alias("ticket_priority_group"),
    )
)
