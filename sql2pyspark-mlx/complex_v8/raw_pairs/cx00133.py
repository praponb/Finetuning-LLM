from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_ticket_closed_a = Window.partitionBy("ticket_id").orderBy(F.col("ticket_closed_at").desc(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_id_by_ticket_closed_a_2 = Window.partitionBy("ticket_id").orderBy(F.col("ticket_closed_at").desc())
w_ticket_closed_at_month_by_ev = Window.partitionBy(F.date_trunc("month", "ticket_closed_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at")
w_event_type_ticket_id_by_tick = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("ticket_closed_at").desc(), "id")

base_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

sequenced = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_closed_at",
        F.round(F.cume_dist().over(w_ticket_id_by_ticket_closed_a), 3).alias("ticket_closed_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.lead("ticket_closed_at", 2).over(w_ticket_id_event_type_by_even).alias("next2_ticket_closed_at"),
        F.round(F.cume_dist().over(w_ticket_id_by_ticket_closed_a_2), 3).alias("ticket_closed_at_cume_dist_2"),
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at"),
        F.lag("ticket_closed_at", 2).over(w_ticket_id_event_type_by_even).alias("prev2_ticket_closed_at"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_ticket_closed_at_month_by_ev).alias("first_known_ticket_closed_at"),
        F.rank().over(w_ticket_id_event_type_by_even_2).alias("event_at_rank"),
        F.lag("ticket_closed_at").over(w_ticket_id_event_type_by_even).alias("prev_ticket_closed_at"),
        F.row_number().over(w_ticket_id_event_type_by_even).alias("seq_no"),
        F.ntile(10).over(w_event_type_ticket_id_by_tick).alias("ticket_closed_at_decile"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_ticket_id_event_type_by_even))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "next2_ticket_closed_at",
        "ticket_closed_at_cume_dist_2",
        "next2_event_at",
        "prev2_ticket_closed_at",
        "first_known_ticket_closed_at",
        "prev_ticket_closed_at",
        "minutes_since_prev_ticket_closed_at",
        F.date_format(F.col("prev2_ticket_closed_at"), "yyyy-MM-dd").alias("prev2_ticket_closed_at_label"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.col("event_at_rank"), F.lit(0)), 3).alias("event_at_cume_dist_per_event_at_rank"),
    )
)
