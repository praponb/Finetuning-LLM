from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_ticket_id = Window.partitionBy(F.year("event_at"), "ticket_id")
w_ticket_csat_actor_by_ticket = Window.partitionBy("ticket_csat", "actor").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_csat_actor_by_ticket_2 = Window.partitionBy("ticket_csat", "actor").orderBy("ticket_csat", "id")
w_all_by_ticket_requester_id = Window.orderBy("ticket_requester_id", "id")
w_all_by_ticket_csat_full = Window.orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_csat_moving_3 = Window.orderBy("ticket_csat", "id").rowsBetween(-3, Window.currentRow)
w_ticket_csat_actor_by_ticket_3 = Window.partitionBy("ticket_csat", "actor").orderBy("ticket_csat")

valid_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
)

joined = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_event_at_year_ticket_id), F.lit(0)), 4).alias("ticket_requester_id_share_of_event_at_year_ticke"),
        F.min("ticket_requester_id").over(w_event_at_year_ticket_id).alias("event_at_year_ticket_id_min_ticket_requester_id"),
        F.first("ticket_csat", ignorenulls=True).over(w_ticket_csat_actor_by_ticket).alias("first_known_ticket_csat"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_event_at_year_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_event_at_year_ticket_id"),
        F.sum("ticket_requester_id").over(w_event_at_year_ticket_id).alias("event_at_year_ticket_id_sum_ticket_requester_id"),
        F.row_number().over(w_ticket_csat_actor_by_ticket_2).alias("seq_no"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_event_at_year_ticket_id), 2).alias("ticket_csat_vs_event_at_year_ticket_id_avg"),
        F.ntile(4).over(w_all_by_ticket_requester_id).alias("ticket_requester_id_quartile"),
        F.last("ticket_requester_id").over(w_all_by_ticket_csat_full).alias("last_ticket_requester_id"),
        F.max("ticket_requester_id").over(w_all_by_ticket_csat_moving_3).alias("ticket_requester_id_max_4"),
        F.max("ticket_requester_id").over(w_event_at_year_ticket_id).alias("event_at_year_ticket_id_max_ticket_requester_id"),
        F.sum("ticket_requester_id").over(w_ticket_csat_actor_by_ticket).alias("running_ticket_requester_id"),
        F.round(F.cume_dist().over(w_ticket_csat_actor_by_ticket_3), 3).alias("ticket_csat_cume_dist"),
    )
)

rollup_base = (
    windowed
    .groupBy("seq_no", "first_known_ticket_csat")
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.count(F.when(F.col("actor") == "customer", 1)).alias("n_actor_customer"),
        F.count(F.when(~F.col("actor").isin("customer", "system"), 1)).alias("n_actor_not_in_customer_system"),
        F.max("ticket_requester_id_share_of_event_at_year_ticke").alias("max_ticket_requester_id_share_of_event_at_year_t"),
    )
)

result = (
    rollup_base
    .select(
        "seq_no",
        "first_known_ticket_csat",
        "n_actor_customer",
        "max_ticket_requester_id_share_of_event_at_year_t",
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
        F.when(F.col("seq_no") >= 9, "large").when(F.col("seq_no") >= 4, "medium").otherwise("small").alias("seq_no_tier"),
    )
    .orderBy(F.col("n_actor_customer").desc())
)
