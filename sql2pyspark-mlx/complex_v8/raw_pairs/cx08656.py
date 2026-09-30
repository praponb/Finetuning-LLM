from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_closed_at_full = Window.orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_year_ticket_id_by_t = Window.partitionBy(F.year("event_at"), "ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_ticket_closed_at_ties = Window.orderBy(F.col("ticket_closed_at").desc())
w_ticket_priority_event_type = Window.partitionBy("ticket_priority", "event_type")
w_event_at_year_ticket_id_by_t_2 = Window.partitionBy(F.year("event_at"), "ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(-3, -1)
w_all_by_event_at_moving_2 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc(), "id")
w_event_at_year_ticket_id_by_t_3 = Window.partitionBy(F.year("event_at"), "ticket_id").orderBy(F.col("ticket_requester_id").desc())
w_distinct_ticket_id_in_ticket = Window.partitionBy("distinct_ticket_id_in_ticket_priority_event_type", "ticket_requester_id_dense_rank")
w_seq_no_ticket_closed_at_to_d = Window.partitionBy("seq_no", F.to_date("ticket_closed_at")).orderBy(F.col("ticket_closed_at_rank").desc(), "id")
w_event_type_ticket_priority_b = Window.partitionBy("event_type", "ticket_priority").orderBy(F.col("ticket_requester_id_sum_3_prior").asc_nulls_last())

cleaned = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") == "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len_2"),
    )
)

enriched_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

ranked = (
    enriched_ticket
    .select(
        "id",
        "event_type",
        "ticket_closed_at",
        "ticket_requester_id",
        "ticket_priority",
        F.last("ticket_closed_at").over(w_all_by_ticket_closed_at_full).alias("last_ticket_closed_at"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_event_at_year_ticket_id_by_t).alias("first_known_ticket_closed_at"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.dense_rank().over(w_all_by_ticket_closed_at_ties).alias("ticket_closed_at_dense_rank"),
        F.size(F.collect_set("ticket_id").over(w_ticket_priority_event_type)).alias("distinct_ticket_id_in_ticket_priority_event_type"),
        F.sum("ticket_requester_id").over(w_event_at_year_ticket_id_by_t_2).alias("ticket_requester_id_sum_3_prior"),
        F.max("ticket_requester_id").over(w_all_by_event_at_moving_2).alias("ticket_requester_id_max_3"),
        F.count(F.lit(1)).over(w_ticket_priority_event_type).alias("rows_in_ticket_priority_event_type"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_priority_event_type), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_priority_eve"),
        F.rank().over(w_all_by_ticket_closed_at).alias("ticket_closed_at_rank"),
        F.dense_rank().over(w_event_at_year_ticket_id_by_t_3).alias("ticket_requester_id_dense_rank"),
    )
)

labeled = (
    ranked
    .filter(F.col("distinct_ticket_id_in_ticket_priority_event_type").between(2, 9))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_priority")), " ", "_")).alias("ticket_priority_slug"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.size(F.collect_set("ticket_priority").over(w_distinct_ticket_id_in_ticket)).alias("distinct_ticket_priority_in_distinct_ticket_id_i"),
        F.size(F.collect_set("event_type").over(w_distinct_ticket_id_in_ticket)).alias("distinct_event_type_in_distinct_ticket_id_in_tic"),
        F.lead("first_known_ticket_closed_at").over(w_seq_no_ticket_closed_at_to_d).alias("next_first_known_ticket_closed_at"),
        F.round(F.cume_dist().over(w_event_type_ticket_priority_b), 3).alias("ticket_requester_id_sum_3_prior_cume_dist"),
        F.count(F.lit(1)).over(w_distinct_ticket_id_in_ticket).alias("rows_in_distinct_ticket_id_in_ticket_priority_ev"),
    )
)

result = (
    with_windows
    .filter(F.col("ticket_requester_id_dense_rank") > 3)
    .select(
        "id",
        "event_type",
        "ticket_closed_at",
        "ticket_requester_id",
        "ticket_priority",
        "last_ticket_closed_at",
        "ticket_closed_at_dense_rank",
        "ticket_requester_id_sum_3_prior",
        "rows_in_ticket_priority_event_type",
        "ticket_requester_id_dense_rank",
        "ticket_priority_slug",
        "distinct_ticket_priority_in_distinct_ticket_id_i",
        "distinct_event_type_in_distinct_ticket_id_in_tic",
        "ticket_requester_id_sum_3_prior_cume_dist",
        F.round((F.unix_timestamp(F.col("last_ticket_closed_at")) - F.unix_timestamp(F.col("ticket_closed_at"))) / 3600, 1).alias("hours_ticket_closed_at_to_last_ticket_closed_at"),
    )
    .orderBy(F.col("ticket_requester_id").desc())
)
