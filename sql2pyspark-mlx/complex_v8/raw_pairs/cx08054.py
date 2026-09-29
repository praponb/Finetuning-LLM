from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_opened_at_ties = Window.orderBy(F.col("ticket_opened_at").desc())
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_all_by_ticket_opened_at = Window.orderBy(F.col("ticket_opened_at").desc(), "id")
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_opened_at_rank_next3 = Window.partitionBy("ticket_opened_at_rank", F.date_trunc("month", "next3_ticket_opened_at"))
w_all_by_next_ticket_opened_at = Window.orderBy(F.col("next_ticket_opened_at").desc(), "id")
w_event_at_quartile_by_event_a = Window.partitionBy("event_at_quartile").orderBy(F.col("event_at_quartile").desc(), "id")
w_all_by_next_ticket_opened_at_2 = Window.orderBy(F.col("next_ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_trends = (
    combined_details_ticket
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_ticket_opened_at_ties), 3).alias("ticket_opened_at_cume_dist"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.lead("ticket_opened_at", 3).over(w_ticket_id_event_type_by_even).alias("next3_ticket_opened_at"),
        F.rank().over(w_all_by_ticket_opened_at).alias("ticket_opened_at_rank"),
        F.lead("ticket_opened_at").over(w_all_by_ticket_opened_at).alias("next_ticket_opened_at"),
        F.round(F.percent_rank().over(w_all_by_ticket_opened_at_ties), 3).alias("ticket_opened_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_event_at_to_date_by_event_at).alias("second_event_at"),
        F.last("ticket_opened_at", ignorenulls=True).over(w_event_at_to_date_by_event_at_2).alias("last_known_ticket_opened_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
    )
)

enriched = (
    with_trends
    .filter(F.col("ticket_opened_at_cume_dist") < 0.8)
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_channel",
        "event_at_quartile",
        "next3_ticket_opened_at",
        "ticket_opened_at_rank",
        "next_ticket_opened_at",
        "ticket_opened_at_percent_rank",
        "second_event_at",
        "last_known_ticket_opened_at",
        "second_event_at_2",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.round(F.col("ticket_opened_at_rank") / F.nullif(F.col("event_at_quartile"), F.lit(0)), 3).alias("ticket_opened_at_rank_per_event_at_quartile"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_opened_at_rank_next3).alias("rows_in_ticket_opened_at_rank_next3_ticket_opene"),
        F.ntile(10).over(w_all_by_next_ticket_opened_at).alias("next_ticket_opened_at_decile"),
        F.lag("second_event_at", 2).over(w_event_at_quartile_by_event_a).alias("prev2_second_event_at"),
        F.min("event_at_quartile").over(w_ticket_opened_at_rank_next3).alias("ticket_opened_at_rank_next3_ticket_opened_at_mon"),
        F.nth_value("next_ticket_opened_at", 2).over(w_all_by_next_ticket_opened_at_2).alias("second_next_ticket_opened_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_channel",
        "event_at_quartile",
        "next3_ticket_opened_at",
        "ticket_opened_at_rank",
        "next_ticket_opened_at",
        "second_event_at",
        "second_event_at_2",
        "event_type_actor_label",
        "ticket_opened_at_rank_per_event_at_quartile",
        "next_ticket_opened_at_decile",
        "ticket_opened_at_rank_next3_ticket_opened_at_mon",
        "second_next_ticket_opened_at",
    )
)
