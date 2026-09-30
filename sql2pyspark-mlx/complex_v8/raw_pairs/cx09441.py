from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_ticket_closed_at_to_date_tic = Window.partitionBy(F.to_date("ticket_closed_at"), "ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_requester_id = Window.orderBy(F.col("ticket_requester_id").desc(), "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 7) & (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "id",
        "event_at",
        "ticket_requester_id",
        "ticket_tags",
        "ticket_closed_at",
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.lead("ticket_closed_at").over(w_ticket_closed_at_to_date_tic).alias("next_ticket_closed_at"),
        F.nth_value("ticket_closed_at", 2).over(w_actor_event_type_by_event_at).alias("second_ticket_closed_at"),
        F.sum("ticket_requester_id").over(w_event_type).alias("event_type_sum_ticket_requester_id"),
        F.lag("event_at").over(w_all_by_ticket_requester_id).alias("prev_event_at"),
        F.lead("ticket_closed_at").over(w_actor_event_type_by_event_at_2).alias("next_ticket_closed_at_2"),
        F.row_number().over(w_all_by_ticket_requester_id).alias("ticket_requester_id_seq"),
        F.count(F.lit(1)).over(w_actor_event_type_by_event_at_3).alias("running_count"),
        F.min("ticket_requester_id").over(w_event_type).alias("event_type_min_ticket_requester_id"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "ticket_tags",
        "ticket_closed_at",
        "rows_in_event_type",
        "next_ticket_closed_at",
        "second_ticket_closed_at",
        "event_type_sum_ticket_requester_id",
        "ticket_requester_id_seq",
        "running_count",
        "event_type_min_ticket_requester_id",
        F.date_format(F.col("next_ticket_closed_at"), "MMM yyyy").alias("next_ticket_closed_at_label"),
    )
)

result = (
    derived
    .filter(F.col("ticket_closed_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_tags",
        "rows_in_event_type",
        "next_ticket_closed_at",
        "running_count",
        "event_type_min_ticket_requester_id",
        "next_ticket_closed_at_label",
    )
)
