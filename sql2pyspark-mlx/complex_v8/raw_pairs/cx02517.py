from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_closed_at = Window.orderBy("ticket_closed_at", "id")
w_event_type_by_ticket_closed = Window.partitionBy("event_type").orderBy("ticket_closed_at")
w_event_type_by_ticket_closed_2 = Window.partitionBy("event_type").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").asc_nulls_last())

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("escalated", "assigned")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

combined_details_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.closed_at") < F.lit("2025-03-12 01:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_ref",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_windows = (
    combined_details_ticket
    .select(
        "*",
        F.lead("ticket_closed_at").over(w_all_by_ticket_closed_at).alias("next_ticket_closed_at"),
        F.round(F.percent_rank().over(w_event_type_by_ticket_closed), 3).alias("ticket_closed_at_percent_rank"),
        F.first("event_at").over(w_event_type_by_ticket_closed_2).alias("first_event_at"),
        F.rank().over(w_event_type_by_ticket_closed).alias("ticket_closed_at_rank"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.lead("ticket_closed_at", 2).over(w_all_by_ticket_closed_at).alias("next2_ticket_closed_at"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

leaders = (
    with_windows
    .filter(F.col("ticket_closed_at_rank") <= 3)
    .drop("ticket_closed_at_rank")
)

result = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_channel",
        "ticket_closed_at",
        "next_ticket_closed_at",
        "ticket_closed_at_percent_rank",
        "next2_ticket_closed_at",
        "event_at_tercile",
        "event_at_percent_rank",
        F.date_trunc("month", F.col("next_ticket_closed_at")).alias("next_ticket_closed_at_month"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy("ticket_id", F.col("event_type").desc(), "id")
    .limit(20)
)
