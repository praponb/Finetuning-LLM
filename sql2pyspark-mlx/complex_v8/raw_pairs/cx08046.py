from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_all_by_ticket_requester_id_t = Window.orderBy(F.col("ticket_requester_id").desc())
w_all_by_ticket_requester_id = Window.orderBy(F.col("ticket_requester_id").desc(), "id")
w_all_by_ticket_requester_id_f = Window.orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_actor_by_ticket_requester_id = Window.partitionBy("actor").orderBy("ticket_requester_id")
w_actor_by_ticket_requester_id_2 = Window.partitionBy("actor").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("escalated", "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

combined_details_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

sequenced = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_requester_id",
        F.sum("ticket_requester_id").over(w_ticket_id).alias("ticket_id_sum_ticket_requester_id"),
        F.min("ticket_requester_id").over(w_ticket_id).alias("ticket_id_min_ticket_requester_id"),
        F.round(F.cume_dist().over(w_all_by_ticket_requester_id_t), 3).alias("ticket_requester_id_cume_dist"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_id), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_id"),
        F.lag("ticket_requester_id", 2, 0).over(w_all_by_ticket_requester_id).alias("prev2_ticket_requester_id"),
        F.nth_value("event_at", 2).over(w_all_by_ticket_requester_id_f).alias("second_event_at"),
        F.last("ticket_requester_id", ignorenulls=True).over(w_actor_ticket_id_by_event_at).alias("last_known_ticket_requester_id"),
        F.lag("event_at").over(w_actor_ticket_id_by_event_at_2).alias("prev_event_at"),
        F.max("ticket_requester_id").over(w_ticket_id).alias("ticket_id_max_ticket_requester_id"),
        F.rank().over(w_actor_by_ticket_requester_id).alias("ticket_requester_id_rank"),
        F.last("ticket_requester_id", ignorenulls=True).over(w_actor_by_ticket_requester_id_2).alias("last_known_ticket_requester_id_2"),
    )
)

labeled = (
    sequenced
    .filter(F.col("ticket_requester_id_share_of_ticket_id") >= 0.5)
    .select(
        "id",
        "event_at",
        "ticket_id_sum_ticket_requester_id",
        "ticket_requester_id_cume_dist",
        "ticket_requester_id_share_of_ticket_id",
        "prev2_ticket_requester_id",
        "second_event_at",
        "last_known_ticket_requester_id",
        "prev_event_at",
        "ticket_requester_id_rank",
        "last_known_ticket_requester_id_2",
        F.when(F.col("ticket_requester_id") >= 17, "A").when(F.col("ticket_requester_id") >= 8, "B").otherwise("C").alias("ticket_requester_id_tier"),
    )
)

result = (
    labeled
    .filter(F.col("ticket_requester_id_share_of_ticket_id") <= 0.6)
    .select(
        "id",
        "event_at",
        "ticket_id_sum_ticket_requester_id",
        "ticket_requester_id_cume_dist",
        "prev2_ticket_requester_id",
        "second_event_at",
        "last_known_ticket_requester_id",
        "last_known_ticket_requester_id_2",
        "ticket_requester_id_tier",
        F.abs(F.col("ticket_requester_id_rank") - 7).alias("ticket_requester_id_rank_distance"),
    )
    .orderBy(
        F.col("last_known_ticket_requester_id").desc_nulls_first(),
        "prev2_ticket_requester_id",
    )
)
