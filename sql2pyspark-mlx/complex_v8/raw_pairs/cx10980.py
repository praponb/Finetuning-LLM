from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_closed_at_full = Window.orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_ticket_c = Window.partitionBy("actor", "event_type").orderBy(F.col("ticket_closed_at").desc(), "id")
w_actor_event_type_by_ticket_c_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_closed_at = Window.orderBy("ticket_closed_at", "id")
w_all_by_ticket_closed_at_runn = Window.orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
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

combined_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

sequenced = (
    combined_details_ticket
    .select(
        "*",
        F.first("ticket_closed_at").over(w_all_by_ticket_closed_at_full).alias("first_ticket_closed_at"),
        F.dense_rank().over(w_actor_event_type_by_ticket_c).alias("ticket_closed_at_dense_rank"),
        F.lead("ticket_closed_at", 3).over(w_actor_event_type_by_ticket_c).alias("next3_ticket_closed_at"),
        F.ntile(5).over(w_actor_event_type_by_ticket_c).alias("ticket_closed_at_quintile"),
        F.nth_value("ticket_closed_at", 2).over(w_all_by_ticket_closed_at_full).alias("second_ticket_closed_at"),
        F.last("ticket_closed_at").over(w_actor_event_type_by_ticket_c_2).alias("last_ticket_closed_at"),
        F.round(F.percent_rank().over(w_all_by_ticket_closed_at), 3).alias("ticket_closed_at_percent_rank"),
        F.lead("ticket_closed_at").over(w_all_by_ticket_closed_at).alias("next_ticket_closed_at"),
        F.row_number().over(w_all_by_ticket_closed_at).alias("rn"),
        F.last("ticket_closed_at", ignorenulls=True).over(w_all_by_ticket_closed_at_runn).alias("last_known_ticket_closed_at"),
    )
)

result = (
    sequenced
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_tags",
        "ticket_closed_at",
        "next3_ticket_closed_at",
        "ticket_closed_at_quintile",
        "ticket_closed_at_percent_rank",
        "next_ticket_closed_at",
        "rn",
        "last_known_ticket_closed_at",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
    .orderBy(F.col("next3_ticket_closed_at").desc())
)
